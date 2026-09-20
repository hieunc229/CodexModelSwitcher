"""Actual loopback HTTP coverage of Azure deadline, cancellation, and replay safety."""
import concurrent.futures
import http.client
import http.server
import json
import socket
import threading
import time
import unittest
import urllib.parse
import urllib.request
from unittest.mock import patch

import test_azure_handler_admission as admission

bridge = admission.bridge


class AzureHandlerDeadlineTests(unittest.TestCase):
    request = admission.AzureHandlerAdmissionTests.request
    stop = admission.AzureHandlerAdmissionTests.stop
    write_effort = admission.AzureHandlerAdmissionTests.write_effort
    configure = admission.AzureHandlerAdmissionTests.configure
    source = admission.AzureHandlerAdmissionTests.source
    inference = admission.AzureHandlerAdmissionTests.inference
    wait_for = admission.AzureHandlerAdmissionTests.wait_for

    def setUp(self):
        admission.AzureHandlerAdmissionTests.setUp(self)
        self.mode = 'headers'
        self.resume_behavior = 'complete'
        self.attempts = 0
        self.methods = []
        self.requests = []
        self.deletes = []
        self.arrived = threading.Event()
        self.disconnected = threading.Event()
        self.end = threading.Event()
        owner = self

        class Upstream(http.server.BaseHTTPRequestHandler):
            def log_message(self, *_args):
                pass

            def do_POST(self):
                body = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
                owner.attempts += 1
                owner.methods.append('POST')
                owner.requests.append(body)
                owner.arrived.set()
                try:
                    if owner.mode == 'headers':
                        self.wfile.write(b'HTTP/1.1 200 OK\r\nX-Trickle: ')
                    elif owner.mode in ('error', 'retryable'):
                        self.send_response(429)
                        self.send_header('Retry-After', '2')
                        self.send_header('Content-Type', 'application/json')
                        self.end_headers()
                        if owner.mode == 'retryable':
                            self.wfile.write(b'{"error":{"message":"synthetic limit"}}')
                            return
                    else:
                        self.send_response(200)
                        self.send_header('Content-Type', 'text/event-stream' if owner.mode in ('stream', 'stream-eof', 'stream-resumable', 'stream-reset', 'backpressure') else 'application/json')
                        if owner.mode == 'verify-truncated':
                            self.send_header('Content-Length', '100')
                        self.end_headers()
                        if owner.mode in ('complete', 'verify-truncated'):
                            self.wfile.write(b'{"status":"completed","output":[]}')
                            return
                        if owner.mode == 'backpressure':
                            event = {'type': 'response.output_text.delta', 'delta': 'x' * (1024 * 1024)}
                            frame = b'data: ' + json.dumps(event).encode() + b'\n\n'
                            for _ in range(32):
                                self.wfile.write(frame)
                        if owner.mode in ('stream-resumable', 'stream-reset'):
                            created = {'type': 'response.created', 'sequence_number': 0,
                                       'response': {'id': 'resp_fixture', 'status': 'in_progress'}}
                            self.wfile.write(b'data: ' + json.dumps(created).encode() + b'\n\n')
                            event = {'type': 'response.output_text.delta', 'sequence_number': 2,
                                     'item_id': 'msg_fixture', 'output_index': 0,
                                     'content_index': 0, 'delta': 'partial-marker'}
                            self.wfile.write(b'data: ' + json.dumps(event).encode() + b'\n\n')
                            self.wfile.flush()
                            if owner.mode == 'stream-reset':
                                self.connection.shutdown(socket.SHUT_RDWR)
                            return
                        if owner.mode in ('stream', 'stream-eof'):
                            event = {'type': 'response.output_text.delta', 'item_id': 'msg_fixture',
                                     'output_index': 0, 'content_index': 0, 'delta': 'partial-marker'}
                            self.wfile.write(b'data: ' + json.dumps(event).encode() + b'\n\n')
                            if owner.mode == 'stream-eof':
                                self.wfile.flush()
                                return
                    self.wfile.flush()
                    self.connection.settimeout(.02)
                    while not owner.end.is_set():
                        try:
                            if not self.connection.recv(1):
                                owner.disconnected.set()
                                return
                        except socket.timeout:
                            if owner.mode in ('headers', 'body', 'error'):
                                self.wfile.write(b' ')
                                self.wfile.flush()
                except OSError:
                    owner.disconnected.set()

            def do_GET(self):
                owner.attempts += 1
                owner.methods.append('GET')
                owner.requests.append(self.path)
                if owner.resume_behavior == '404-race' and 'starting_after=' in self.path:
                    self.send_error(404)
                    return
                if owner.resume_behavior == 'retrieval-reset':
                    if 'starting_after=' in self.path:
                        self.send_error(404)
                    else:
                        self.connection.shutdown(socket.SHUT_RDWR)
                    return
                if owner.resume_behavior == 'always-404':
                    self.send_error(404)
                    return
                if owner.resume_behavior == '404-race':
                    self.send_response(200)
                    self.send_header('Content-Type', 'application/json')
                    self.end_headers()
                    self.wfile.write(json.dumps({'id': 'resp_fixture', 'status': 'completed',
                        'output': [{'type': 'message', 'content': [
                            {'type': 'output_text', 'text': 'recovered-race'}]}]}).encode())
                    return
                self.send_response(200)
                self.send_header('Content-Type', 'text/event-stream')
                self.end_headers()
                completed = {'type': 'response.completed', 'sequence_number': 3,
                    'response': {'id': 'resp_fixture', 'status': 'completed',
                                 'output': [{'type': 'message', 'content': [
                                     {'type': 'output_text', 'text': 'recovered'}]}]}}
                self.wfile.write(b'data: ' + json.dumps(completed).encode() + b'\n\n')

            def do_DELETE(self):
                owner.methods.append('DELETE')
                owner.deletes.append(self.path)
                self.send_response(200)
                self.send_header('Content-Type', 'application/json')
                self.end_headers()
                self.wfile.write(b'{"deleted":true}')

        upstream = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Upstream)
        upstream.daemon_threads = True
        runner = threading.Thread(target=lambda: upstream.serve_forever(poll_interval=.01), daemon=True)
        runner.start()

        def cleanup():
            self.end.set()
            upstream.shutdown()
            upstream.server_close()
            runner.join(2)
        self.addCleanup(cleanup)
        original_builder = urllib.request.build_opener

        def loopback_builder(*handlers):
            opener = original_builder(urllib.request.ProxyHandler({}), *handlers)
            class Loopback:
                def open(self, request, **kwargs):
                    parsed = urllib.parse.urlsplit(request.full_url)
                    path = parsed.path + (('?' + parsed.query) if parsed.query else '')
                    redirected = urllib.request.Request(
                        f'http://127.0.0.1:{upstream.server_port}{path}', data=request.data,
                        headers=dict(request.header_items()), method=request.get_method())
                    return opener.open(redirected, **kwargs)
            return Loopback()

        for name, value in [('AZURE_REQUEST_SECONDS', .35), ('AZURE_VERIFY_SECONDS', .35)]:
            patcher = patch.object(bridge, name, value)
            patcher.start()
            self.addCleanup(patcher.stop)
        patcher = patch.object(bridge.urllib.request, 'build_opener', side_effect=loopback_builder)
        patcher.start()
        self.addCleanup(patcher.stop)

    def finished(self):
        self.wait_for(lambda: self.runtime.status()['active_requests'] == 0)
        self.assertEqual(bridge.AZURE_ADMISSION.snapshot()['keys'], 0)

    def test_trickling_headers_deadline_closes_socket_and_retains_uncertain_turn(self):
        began = time.monotonic()
        status, body = self.inference('trickle-headers')
        self.assertEqual(status, 504)
        self.assertIn(b'total deadline', body)
        self.assertLess(time.monotonic() - began, 2)
        self.assertTrue(self.disconnected.wait(1))
        self.finished()
        self.assertEqual(self.runtime.status()['uncertain_turns'], 1)
        self.assertEqual(self.inference('trickle-headers')[0], 400)
        self.assertEqual(self.attempts, 1)

    def test_json_body_trickle_returns_valid_deadline_error_before_success_headers(self):
        self.mode = 'body'
        status, body = self.inference('json-stall')
        self.assertEqual(status, 504)
        self.assertIn('error', json.loads(body))
        self.assertNotIn(b'event:', body)
        self.assertTrue(self.disconnected.wait(1))
        self.finished()
        self.assertEqual(self.runtime.status()['uncertain_turns'], 1)
        self.assertEqual(self.attempts, 1)

    def test_partial_stream_deadline_reports_failure_without_replaying_output(self):
        self.mode = 'stream'
        status, body = self.inference('stream-stall', stream=True)
        self.assertEqual(status, 200)
        self.assertEqual(body.count(b'partial-marker'), 1)
        self.assertIn(b'response.failed', body)
        self.assertIn(b'total deadline', body)
        self.assertTrue(self.disconnected.wait(1))
        self.finished()
        self.assertEqual(self.runtime.status()['uncertain_turns'], 1)
        self.assertEqual(self.inference('stream-stall', stream=True)[0], 400)
        self.assertEqual(self.attempts, 1)

    def test_partial_stream_eof_reports_failure_and_preserves_uncertain_delivery(self):
        self.mode = 'stream-eof'
        status, body = self.inference('stream-eof', stream=True)
        self.assertEqual(status, 200)
        self.assertEqual(body.count(b'partial-marker'), 1)
        self.assertIn(b'response.failed', body)
        self.assertIn(b'ended before a terminal response event', body)
        self.assertNotIn(b'response.completed', body)
        self.finished()
        self.assertEqual(self.runtime.status()['uncertain_turns'], 1)
        self.assertEqual(self.inference('stream-eof', stream=True)[0], 400)
        self.assertEqual(self.attempts, 1)

    def test_initial_pre_body_failure_retries_once_without_duplicate_dispatch(self):
        class Response:
            status = 200
            headers = {'Content-Type': 'application/json'}
            def __enter__(self): return self
            def __exit__(self, *_): return False
            def read(self): return b'{"status":"completed","output":[{"type":"message","content":[{"type":"output_text","text":"OK"}]}]}'

        original_budget = bridge._transport_module.RequestBudget
        budgets = []
        def tracked_budget(*args, **kwargs):
            budget = original_budget(*args, **kwargs)
            budgets.append(budget)
            return budget
        calls = []
        def upstream(request, **_kwargs):
            calls.append(request)
            if len(calls) == 1:
                raise ConnectionResetError('synthetic reset before model bytes')
            budget = budgets[-1]
            budget.mark_request_headers_possible()
            budget.mark_response_headers_received()
            return Response()

        with patch.object(bridge._transport_module, 'RequestBudget', side_effect=tracked_budget), \
             patch.object(bridge.urllib.request, 'build_opener') as opener:
            opener.return_value.open.side_effect = upstream
            status, body = self.inference('retry-before-body')
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(body)['status'], 'completed')
        self.assertEqual(len(calls), 2)
        self.assertEqual(self.runtime.status()['uncertain_turns'], 0)
        self.assertEqual(bridge.AZURE_ADMISSION.snapshot()['keys'], 0)
        self.assertEqual(len(budgets), 1)
        self.assertTrue(budgets[0].dispatch_possible)

    def test_two_initial_pre_body_failures_are_replayable_and_record_attempt_count(self):
        original_budget = bridge._transport_module.RequestBudget
        budgets = []
        def tracked_budget(*args, **kwargs):
            budget = original_budget(*args, **kwargs)
            budgets.append(budget)
            return budget
        calls = []
        def upstream(request, **_kwargs):
            calls.append(request)
            raise ConnectionResetError('synthetic reset before model bytes')

        with patch.object(bridge._transport_module, 'RequestBudget', side_effect=tracked_budget), \
             patch.object(bridge.urllib.request, 'build_opener') as opener:
            opener.return_value.open.side_effect = upstream
            status, body = self.inference('retry-exhausted-before-body')
        self.assertEqual(status, 502)
        self.assertIn(b'response connection was interrupted', body)
        self.assertEqual(len(calls), 2)
        self.finished()
        self.assertEqual(self.runtime.status()['uncertain_turns'], 0)
        failure = bridge.RECENT_FAILURES[-1]
        self.assertEqual(failure['phase'], 'pre_model_body')
        self.assertEqual(failure['initial_post_attempts'], 2)

    def test_initial_post_body_failure_never_replays_and_records_safe_diagnostics(self):
        original_budget = bridge._transport_module.RequestBudget
        budgets = []
        def tracked_budget(*args, **kwargs):
            budget = original_budget(*args, **kwargs)
            budgets.append(budget)
            return budget
        calls = []
        def upstream(request, **_kwargs):
            calls.append(request)
            budgets[-1].mark_model_bytes_possible()
            raise ConnectionResetError('synthetic reset after PRIVATE_PROMPT_MARKER')

        with patch.object(bridge._transport_module, 'RequestBudget', side_effect=tracked_budget), \
             patch.object(bridge.urllib.request, 'build_opener') as opener:
            opener.return_value.open.side_effect = upstream
            status, body = self.inference('PRIVATE_PROMPT_MARKER')
        self.assertEqual(status, 502)
        self.assertIn(b'response connection was interrupted', body)
        self.assertEqual(len(calls), 1)
        self.finished()
        self.assertEqual(self.runtime.status()['uncertain_turns'], 1)
        self.assertEqual(self.inference('PRIVATE_PROMPT_MARKER')[0], 400)
        failure = bridge.RECENT_FAILURES[-1]
        self.assertEqual(failure['kind'], 'connection_interrupted')
        self.assertEqual(failure['phase'], 'post_dispatch_pre_header')
        self.assertEqual(failure['initial_post_attempts'], 1)
        encoded = json.dumps(failure)
        self.assertNotIn('PRIVATE_PROMPT_MARKER', encoded)
        self.assertNotIn(self.key, encoded)

    def test_compaction_shaped_post_dispatch_failure_is_content_free_and_not_replayed(self):
        original_budget = bridge._transport_module.RequestBudget
        budgets = []

        def tracked_budget(*args, **kwargs):
            budget = original_budget(*args, **kwargs)
            budgets.append(budget)
            return budget

        calls = []

        def upstream(request, **_kwargs):
            calls.append(request)
            budgets[-1].mark_model_bytes_possible()
            raise ConnectionResetError('PRIVATE_COMPACTION_MARKER and provider secret')

        source = {
            'model': 'harbor/azure/coding-prod',
            'input': [{'type': 'compaction', 'encrypted_content': 'PRIVATE_COMPACTION_MARKER'}],
            'stream': False,
        }
        turn_metadata = json.dumps({
            'thread_id': 'fixture-task',
            'turn_id': 'compaction-turn',
            'request_kind': 'compaction',
            'compaction': {'phase': 'pre_turn'},
        })

        def request():
            connection = http.client.HTTPConnection('127.0.0.1', self.server.server_port, timeout=4)
            try:
                connection.request('POST', '/harbor/v1/responses', json.dumps(source),
                                   {'Authorization': 'Bearer synthetic-owner-token',
                                    'x-codex-turn-metadata': turn_metadata})
                response = connection.getresponse()
                return response.status, response.read()
            finally:
                connection.close()

        with patch.object(bridge._transport_module, 'RequestBudget', side_effect=tracked_budget), \
             patch.object(bridge.urllib.request, 'build_opener') as opener:
            opener.return_value.open.side_effect = upstream
            status, body = request()

        self.assertEqual(status, 502)
        self.assertIn(b'upstream Azure connection was interrupted', body)
        self.assertEqual(len(calls), 1)
        self.finished()
        self.assertEqual(self.runtime.status()['uncertain_turns'], 1)
        self.assertEqual(request()[0], 400)
        self.assertEqual(len(calls), 1)
        failure = bridge.RECENT_FAILURES[-1]
        self.assertEqual(failure['side'], 'upstream')
        self.assertEqual(failure['classification'], 'provider_connection_lost')
        self.assertEqual(failure['request_kind'], 'compaction')
        self.assertEqual(failure['compaction_phase'], 'pre_turn')
        encoded = json.dumps(failure)
        self.assertNotIn('PRIVATE_COMPACTION_MARKER', encoded)
        self.assertNotIn('provider secret', encoded)
        self.assertNotIn(self.key, encoded)

    def test_compaction_shaped_provider_http_error_is_classified_and_known(self):
        self.mode = 'retryable'
        source = {
            'model': 'harbor/azure/coding-prod',
            'input': [{'type': 'compaction', 'encrypted_content': 'PRIVATE_COMPACTION_MARKER'}],
            'stream': False,
        }
        turn_metadata = json.dumps({
            'thread_id': 'fixture-task',
            'turn_id': 'compaction-http-error',
            'request_kind': 'compaction',
            'compaction': {'phase': 'pre_turn'},
        })

        def request():
            connection = http.client.HTTPConnection('127.0.0.1', self.server.server_port, timeout=4)
            try:
                connection.request('POST', '/harbor/v1/responses', json.dumps(source),
                                   {'Authorization': 'Bearer synthetic-owner-token',
                                    'x-codex-turn-metadata': turn_metadata})
                response = connection.getresponse()
                return response.status, response.read()
            finally:
                connection.close()

        status, _body = request()
        self.assertEqual(status, 429)
        self.finished()
        self.assertEqual(self.runtime.status()['uncertain_turns'], 0)
        failure = bridge.RECENT_FAILURES[-1]
        self.assertEqual(failure['side'], 'upstream')
        self.assertEqual(failure['classification'], 'provider_http_error')
        self.assertEqual(failure['kind'], 'provider_http_error')
        self.assertEqual(failure['request_kind'], 'compaction')
        self.assertEqual(failure['compaction_phase'], 'pre_turn')
        encoded = json.dumps(failure)
        self.assertNotIn('PRIVATE_COMPACTION_MARKER', encoded)
        self.assertNotIn(self.key, encoded)

    def test_background_azure_stream_resumes_after_eof_without_replaying_post(self):
        self.mode = 'stream-resumable'
        self.write_effort('none', resumable=True)
        status, body = self.inference('stream-resumable', stream=True)
        self.assertEqual(status, 200)
        self.assertIn(b'partial-marker', body)
        self.assertIn(b'recovered', body)
        self.assertIn(b'response.completed', body)
        self.assertNotIn(b'response.failed', body)
        self.finished()
        self.assertEqual(self.runtime.status()['uncertain_turns'], 0)
        self.wait_for(lambda: bool(self.deletes))
        self.assertEqual(self.methods[:2], ['POST', 'GET'])
        self.assertEqual(self.methods.count('POST'), 1)
        self.assertTrue(self.requests[0]['background'])
        self.assertTrue(self.requests[0]['store'])
        self.assertEqual(self.requests[1], '/openai/v1/responses/resp_fixture?stream=true&starting_after=2')
        self.assertEqual(self.deletes, ['/openai/v1/responses/resp_fixture'])

    def test_background_azure_stream_resumes_after_connection_reset(self):
        self.mode = 'stream-reset'
        self.write_effort('none', resumable=True)
        status, body = self.inference('stream-reset', stream=True)
        self.assertEqual(status, 200)
        self.assertIn(b'response.completed', body)
        self.assertEqual(self.methods.count('POST'), 1)
        self.assertEqual(self.methods[1], 'GET')

    def test_resume_404_completion_race_retrieves_same_response(self):
        self.mode = 'stream-resumable'
        self.resume_behavior = '404-race'
        self.write_effort('none', resumable=True)
        status, body = self.inference('stream-race', stream=True)
        self.assertEqual(status, 200)
        self.assertIn(b'recovered-race', body)
        self.assertIn(b'response.completed', body)
        self.assertEqual(self.methods[:3], ['POST', 'GET', 'GET'])
        self.assertEqual(self.methods.count('POST'), 1)
        self.assertNotIn('starting_after=', self.requests[2])

    def test_resume_exhaustion_never_replays_post_and_deletes_response(self):
        self.mode = 'stream-resumable'
        self.resume_behavior = 'always-404'
        self.write_effort('none', resumable=True)
        status, body = self.inference('stream-exhausted', stream=True)
        self.assertEqual(status, 200)
        self.assertIn(b'response.failed', body)
        self.assertEqual(self.methods.count('POST'), 1)
        self.assertEqual(self.methods.count('GET'), bridge.AZURE_STREAM_RESUMES * 2)
        self.wait_for(lambda: bool(self.deletes))
        self.assertEqual(self.deletes, ['/openai/v1/responses/resp_fixture'])

    def test_resume_connection_errors_exhaust_without_replaying_post(self):
        self.mode = 'stream-resumable'
        self.write_effort('none', resumable=True)
        original_builder = urllib.request.build_opener
        opened = original_builder(urllib.request.ProxyHandler({}))
        class ErrorsAfterPost:
            def open(self, request, **kwargs):
                if request.get_method() == 'POST':
                    return opened.open(request, **kwargs)
                raise ConnectionResetError('synthetic resume reset')
        with patch.object(bridge.urllib.request, 'build_opener', return_value=ErrorsAfterPost()):
            status, body = self.inference('resume-reset-exhausted', stream=True)
        self.assertEqual(status, 200)
        self.assertIn(b'response.failed', body)
        self.assertEqual(self.attempts, 1)

    def test_completion_race_retrieval_errors_are_bounded_without_replaying_post(self):
        self.mode = 'stream-resumable'
        self.resume_behavior = 'retrieval-reset'
        self.write_effort('none', resumable=True)
        status, body = self.inference('retrieval-reset-exhausted', stream=True)
        self.assertEqual(status, 200)
        self.assertIn(b'response.failed', body)
        self.assertEqual(self.methods.count('POST'), 1)
        self.assertEqual(self.methods.count('GET'), bridge.AZURE_STREAM_RESUMES * 2)

    def test_client_cancellation_during_resume_never_replays_post_and_deletes(self):
        self.mode = 'stream-resumable'
        self.write_effort('none', resumable=True)
        cancelled = threading.Event()
        cleanup_started = threading.Event()

        class CancelledResponse:
            status = 200
            headers = {'Content-Type': 'text/event-stream'}
            def __enter__(self): return self
            def __exit__(self, *_): return False
            def __iter__(self):
                yield b'data: {"type":"response.created","sequence_number":0,"response":{"id":"resp_cancel","status":"in_progress"}}\n'
                yield b'\n'
                cancelled.set()
                raise ConnectionResetError('synthetic reset after cancellation')

        original_builder = urllib.request.build_opener
        delete_client = original_builder(urllib.request.ProxyHandler({}))
        class Opener:
            def open(self, request, **_kwargs):
                if request.get_method() == 'DELETE':
                    cleanup_started.set()
                    return delete_client.open(request, **_kwargs)
                return CancelledResponse()

        original_disconnected = bridge.Handler.client_disconnected
        def disconnected(handler):
            return cancelled.is_set() or original_disconnected(handler)
        with patch.object(bridge.urllib.request, 'build_opener', return_value=Opener()), \
             patch.object(bridge.Handler, 'client_disconnected', disconnected):
            status, body = self.inference('cancel-resume', stream=True)
        self.assertEqual(status, 200)
        self.assertNotIn(b'response.completed', body)
        self.assertEqual(self.methods.count('GET'), 0)
        self.finished()
        self.assertTrue(cleanup_started.wait(1))
        self.assertEqual(self.runtime.status()['uncertain_turns'], 0)

    def test_heartbeat_sequence_does_not_change_raw_resume_cursor(self):
        self.mode = 'stream-resumable'
        self.write_effort('none', resumable=True)
        original = bridge.Translation.event
        def inject_heartbeat(translation, block):
            result = original(translation, block)
            if translation.upstream_sequence == 2:
                translation.lifecycle.injected = True
                translation.lifecycle.sequence = 99
            return result
        with patch.object(bridge.Translation, 'event', inject_heartbeat):
            status, _ = self.inference('stream-heartbeat-cursor', stream=True)
        self.assertEqual(status, 200)
        self.assertIn('starting_after=2', self.requests[1])

    def test_nonstream_and_unverified_stream_remain_unstored(self):
        self.mode = 'complete'
        status, _ = self.inference('nonstream')
        self.assertEqual(status, 200)
        self.assertFalse(self.requests[0]['store'])
        self.assertNotIn('background', self.requests[0])
        self.methods.clear(); self.requests.clear(); self.attempts = 0
        self.mode = 'stream-eof'
        status, body = self.inference('ordinary-stream', stream=True)
        self.assertEqual(status, 200)
        self.assertIn(b'response.failed', body)
        self.assertEqual(self.methods, ['POST'])
        self.assertFalse(self.requests[0]['store'])
        self.assertNotIn('background', self.requests[0])

    def test_resume_url_encodes_response_identifier(self):
        request = bridge.azure_resume_request('https://fixture.openai.azure.com/openai/v1',
            {'api-key': 'fixture'}, 'resp /?#%', 7)
        self.assertEqual(request.full_url,
            'https://fixture.openai.azure.com/openai/v1/responses/resp%20%2F%3F%23%25?stream=true&starting_after=7')

    def test_recovery_diagnostics_are_content_free(self):
        self.mode = 'stream-resumable'
        self.write_effort('none', resumable=True)
        status, _ = self.inference('private prompt marker', stream=True)
        self.assertEqual(status, 200)
        status, body = self.request('GET', '/harbor/status')
        self.assertEqual(status, 200)
        diagnostics = body['azure_stream_recovery']
        self.assertGreaterEqual(diagnostics['attempted'], 1)
        self.assertGreaterEqual(diagnostics['succeeded'], 1)
        encoded = json.dumps(diagnostics)
        self.assertNotIn('private prompt marker', encoded)
        self.assertNotIn('resp_fixture', encoded)
        self.assertNotIn(self.key, encoded)

    def test_error_body_stall_is_not_a_known_delivered_outcome(self):
        self.mode = 'error'
        status, body = self.inference('error-stall')
        self.assertEqual(status, 504)
        self.assertIn(b'total deadline', body)
        self.assertTrue(self.disconnected.wait(1))
        self.finished()
        self.assertEqual(self.runtime.status()['uncertain_turns'], 1)
        self.assertEqual(self.attempts, 1)

    def test_queued_deadline_never_dispatches_and_preserves_existing_readiness(self):
        queue = bridge._admission_module.AdmissionQueue(active_limit=1)
        route = {'provider': 'azure', 'model': 'coding-prod'}
        bridge.record_readiness(route, bridge.configuration_revision(), 'verified')
        with patch.object(bridge, 'AZURE_ADMISSION', queue):
            held = queue.acquire((self.endpoint, 'coding-prod'))
            try:
                status, _body = self.inference('queued-deadline')
                self.assertEqual(status, 504)
                self.wait_for(lambda: self.runtime.status()['active_requests'] == 0)
                self.assertEqual(self.runtime.status()['uncertain_turns'], 0)
                status, body = self.request(path='/harbor/verify', body={'model': 'harbor/azure/coding-prod'})
                self.assertEqual((status, body['result']), (503, 'busy'))
                self.assertTrue(bridge.provider_status()['azure_ready'])
                self.assertEqual(self.attempts, 0)
            finally:
                held.release()
            self.mode = 'complete'
            self.assertEqual(self.inference('queued-deadline')[0], 200)
            self.finished()
            self.assertEqual(self.attempts, 1)

    def test_queue_and_upstream_share_the_same_budget_instance(self):
        queue = bridge._admission_module.AdmissionQueue(active_limit=1)
        budgets = []
        original_budget = bridge._transport_module.RequestBudget
        def tracked_budget(*args, **kwargs):
            budget = original_budget(*args, **kwargs)
            budgets.append(budget)
            return budget
        with patch.object(bridge, 'AZURE_ADMISSION', queue), \
             patch.object(bridge._transport_module, 'RequestBudget', side_effect=tracked_budget):
            held = queue.acquire((self.endpoint, 'coding-prod'))
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as workers:
                result = workers.submit(self.inference, 'queue-then-stall')
                try:
                    self.wait_for(lambda: queue.snapshot()['waiting'] == 1)
                    first_deadline = budgets[0].deadline
                    time.sleep(.12)
                finally:
                    held.release()
                self.assertEqual(result.result(timeout=2)[0], 504)
            self.finished()
        self.assertEqual(len(budgets), 2)
        self.assertFalse(budgets[1].dispatch_possible)
        self.assertEqual(budgets[0].deadline, first_deadline)
        self.assertEqual(budgets[0].stop_reason, 'deadline')
        self.assertEqual(self.attempts, 1)

    def test_client_disconnect_interrupts_blocked_upstream_and_releases_slot(self):
        with patch.object(bridge, 'AZURE_REQUEST_SECONDS', 5):
            client = http.client.HTTPConnection('127.0.0.1', self.server.server_port, timeout=2)
            try:
                client.request('POST', '/harbor/v1/responses', json.dumps(self.source('cancel-upstream')),
                               {'Authorization': 'Bearer synthetic-owner-token'})
                self.assertTrue(self.arrived.wait(1))
                client.sock.shutdown(socket.SHUT_RDWR)
                client.close()
                self.assertTrue(self.disconnected.wait(1))
                self.finished()
            finally:
                client.close()
        self.assertEqual(self.runtime.status()['uncertain_turns'], 1)
        self.assertEqual(self.attempts, 1)
        failure = bridge.RECENT_FAILURES[-1]
        self.assertEqual(failure['side'], 'downstream')
        self.assertEqual(failure['classification'], 'client_disconnected')
        self.assertEqual(failure['kind'], 'client_disconnected')

    def test_upstream_reset_is_reported_as_provider_failure_not_client_disconnect(self):
        original_budget = bridge._transport_module.RequestBudget

        def dispatched_budget(*args, **kwargs):
            budget = original_budget(*args, **kwargs)
            budget.mark_model_bytes_possible()
            return budget

        with patch.object(bridge._transport_module, 'RequestBudget', side_effect=dispatched_budget), \
             patch.object(bridge.urllib.request, 'build_opener') as opener:
            opener.return_value.open.side_effect = ConnectionResetError('private upstream detail')
            status, body = self.inference('provider-reset')
        self.assertEqual(status, 502)
        self.assertIn(b'upstream Azure connection', body)
        self.finished()
        failure = bridge.RECENT_FAILURES[-1]
        self.assertEqual(failure['side'], 'upstream')
        self.assertEqual(failure['classification'], 'provider_connection_lost')
        self.assertNotIn('private upstream detail', json.dumps(failure))

    def test_verification_rejects_completed_json_with_truncated_http_framing(self):
        self.mode = 'verify-truncated'
        route = {'provider': 'azure', 'model': 'coding-prod'}
        bridge.record_readiness(route, bridge.configuration_revision(), 'verified')
        status, body = self.request(path='/harbor/verify', body={'model': 'harbor/azure/coding-prod'})
        self.assertEqual((status, body['result']), (503, 'unavailable'))
        self.assertFalse(bridge.provider_status()['azure_ready'])
        self.assertEqual(bridge.AZURE_ADMISSION.snapshot()['keys'], 0)
        self.assertEqual(self.attempts, 1)

    def test_dispatched_verification_timeout_invalidates_stale_proof(self):
        route = {'provider': 'azure', 'model': 'coding-prod'}
        bridge.record_readiness(route, bridge.configuration_revision(), 'verified')
        status, body = self.request(path='/harbor/verify', body={'model': 'harbor/azure/coding-prod'})
        self.assertEqual((status, body['result']), (503, 'unavailable'))
        self.assertFalse(bridge.provider_status()['azure_ready'])
        self.assertTrue(self.disconnected.wait(1))
        self.assertEqual(bridge.AZURE_ADMISSION.snapshot()['keys'], 0)
        self.assertEqual(self.attempts, 1)

    def test_nonreading_client_cannot_hold_an_upstream_slot_past_deadline(self):
        self.mode = 'backpressure'
        writing = threading.Event()
        original_write = bridge.Handler.write_output
        def small_buffer(handler, value, budget=None, **kwargs):
            if len(value) > 512 * 1024:
                handler.connection.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4096)
                writing.set()
            return original_write(handler, value, budget, **kwargs)
        client = socket.socket()
        client.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1024)
        client.settimeout(2)
        client.connect(('127.0.0.1', self.server.server_port))
        with patch.object(bridge.Handler, 'write_output', small_buffer):
            try:
                body = json.dumps(self.source('blocked-delivery', stream=True)).encode()
                client.sendall(b'POST /harbor/v1/responses HTTP/1.1\r\nHost: localhost\r\n'
                               b'Authorization: Bearer synthetic-owner-token\r\n'
                               + f'Content-Length: {len(body)}\r\n\r\n'.encode() + body)
                self.assertTrue(writing.wait(1))
                self.finished()
                self.assertTrue(self.disconnected.wait(1))
            finally:
                client.close()
        self.assertEqual(self.runtime.status()['uncertain_turns'], 1)
        self.assertEqual(self.attempts, 1)

    def test_completed_error_is_delivered_once_and_is_not_uncertain(self):
        self.mode = 'retryable'
        connection = http.client.HTTPConnection('127.0.0.1', self.server.server_port, timeout=4)
        held_socket = None
        try:
            connection.request('POST', '/harbor/v1/responses', json.dumps(self.source('retryable-once')),
                               {'Authorization': 'Bearer synthetic-owner-token'})
            # HTTPResponse closes its socket after reading a Connection: close body.
            # Keep this client connected until the handler's delivery checks finish.
            held_socket = connection.sock.dup()
            with connection.getresponse() as response:
                body = response.read()
                self.assertEqual(response.status, 429)
            self.assertEqual(json.loads(body)['error']['message'], 'synthetic limit')
            self.finished()
            self.assertEqual(self.runtime.status()['uncertain_turns'], 0)
            self.assertEqual(self.attempts, 1)
            failure = bridge.RECENT_FAILURES[-1]
            self.assertEqual(failure['classification'], 'provider_http_error')
            self.assertEqual(failure['outcome'], 'known')
        finally:
            connection.close()
            if held_socket is not None:
                held_socket.close()


if __name__ == '__main__':
    unittest.main()
