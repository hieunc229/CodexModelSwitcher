import Foundation

struct APIConnectionPresentation {
    let credentialsAvailable: Bool
    let proofs: [[String: Any]]
    let provider: String
    let revision: String
    let bootID: String

    private var current: [[String: Any]] {
        proofs.filter { $0["provider"] as? String == provider
            && $0["configuration_revision"] as? String == revision
            && $0["boot_id"] as? String == bootID }
    }
    var connected: Bool {
        credentialsAvailable && current.contains { $0["result"] as? String == "verified"
            && $0["verified"] as? Bool == true }
            && !current.contains { $0["result"] as? String == "auth_failed" }
    }
    var label: String {
        if current.contains(where: { $0["result"] as? String == "auth_failed" }) { return "Reconnect required" }
        if connected { return "Connected" }
        return credentialsAvailable ? "Configured" : "Not connected"
    }
}

struct ProviderDefinition: Identifiable {
    let id: String
    let name: String
    let symbol: String
    let method: String
    let dashboard: String
    static let all: [ProviderDefinition] = [
        .init(id: "codex-subscription", name: "Codex", symbol: "terminal", method: "ChatGPT subscription", dashboard: "https://chatgpt.com"),
        .init(id: "azure", name: "Azure", symbol: "cloud", method: "Azure OpenAI API", dashboard: "https://ai.azure.com"),
        .init(id: "baseten", name: "Baseten", symbol: "square.stack.3d.up", method: "Direct API", dashboard: "https://app.baseten.co"),
        .init(id: "grok-oauth", name: "Grok", symbol: "sparkle", method: "Browser sign-in", dashboard: "https://grok.com"),
        .init(id: "openrouter", name: "OpenRouter", symbol: "arrow.triangle.branch", method: "API key", dashboard: "https://openrouter.ai/settings/keys")
    ]
    static func named(_ id: String) -> ProviderDefinition { all.first { $0.id == id } ?? all[0] }
}

enum MenuBarDisplay: String, CaseIterable, Identifiable {
    case connection, activity, quota, spend, reset, model, count, name, icon
    var id: String { rawValue }
    var title: String {
        switch self {
        case .connection: return "Provider + connection"
        case .activity: return "Provider + activity"
        case .quota: return "Provider + quota remaining"
        case .spend: return "Provider + today's spend"
        case .reset: return "Provider + next reset"
        case .model: return "Last requested model"
        case .count: return "Connected provider count"
        case .name: return "Harbor name"
        case .icon: return "Icon only"
        }
    }
}

struct ProviderActivity {
    var active = 0
    var completed = 0
    var failed = 0
    var model = ""
    var state = "idle"
    var lastSuccess: Date?
    var lastFailure: Date?
    var lastAuthFailure: Date?
    var httpStatus: Int?
    var verifiedConnection: Bool {
        guard let lastSuccess else { return false }
        let authFailure = lastAuthFailure ?? ([401, 403].contains(httpStatus ?? 0) ? lastFailure : nil)
        return (authFailure ?? .distantPast) < lastSuccess
    }
    init(_ raw: [String: Any] = [:]) {
        active = raw["active"] as? Int ?? 0
        completed = raw["completed"] as? Int ?? 0
        failed = raw["failed"] as? Int ?? 0
        model = raw["model"] as? String ?? ""
        state = raw["state"] as? String ?? "idle"
        lastSuccess = (raw["last_success"] as? Double).map(Date.init(timeIntervalSince1970:))
        lastFailure = (raw["last_failure"] as? Double).map(Date.init(timeIntervalSince1970:))
        lastAuthFailure = (raw["last_auth_failure"] as? Double).map(Date.init(timeIntervalSince1970:))
        httpStatus = raw["http_status"] as? Int
    }
}

struct OpenRouterModel: Identifiable, Equatable {
    let id: String
    let name: String
    let context: Int
    let reasoning: Bool
    let vision: Bool
    static func parse(_ data: Data) throws -> [OpenRouterModel] {
        let object = try JSONSerialization.jsonObject(with: data) as? [String: Any]
        guard let entries = object?["data"] as? [[String: Any]] else { throw ProviderError.message("OpenRouter returned an invalid model catalog.") }
        return entries.compactMap { entry in
            guard let id = entry["id"] as? String, let name = entry["name"] as? String,
                  let supported = entry["supported_parameters"] as? [String], supported.contains("tools") else { return nil }
            let architecture = entry["architecture"] as? [String: Any]
            return OpenRouterModel(id: id, name: name, context: entry["context_length"] as? Int ?? 128000,
                                   reasoning: supported.contains("reasoning"),
                                   vision: (architecture?["input_modalities"] as? [String] ?? []).contains("image"))
        }.sorted { $0.name.localizedCaseInsensitiveCompare($1.name) == .orderedAscending }
    }
    var catalogEntry: [String: Any] {
        ["slug": id, "display_name": name,
         "base_instructions": "You are a coding assistant. Follow instructions, use tools, and verify your work.",
         "default_reasoning_level": reasoning ? "high" : "none",
         "supported_reasoning_levels": (reasoning ? ["low", "medium", "high"] : ["none"]).map { ["effort": $0, "description": $0.capitalized] },
         "shell_type": "shell_command", "context_window": context, "max_context_window": context,
         "input_modalities": vision ? ["text", "image"] : ["text"], "support_verbosity": false,
         "truncation_policy": ["mode": "tokens", "limit": 10000], "experimental_supported_tools": []]
    }
}

enum ProviderError: LocalizedError {
    case message(String)
    var errorDescription: String? { if case .message(let message) = self { return message }; return nil }
}

enum OpenRouterAPI {
    static func models(key: String) async throws -> [OpenRouterModel] {
        _ = try await request("key", key: key)
        return try OpenRouterModel.parse(await request("models", key: key))
    }
    private static func request(_ path: String, key: String) async throws -> Data {
        var request = URLRequest(url: URL(string: "https://openrouter.ai/api/v1/\(path)")!, timeoutInterval: 30)
        request.setValue("Bearer \(key)", forHTTPHeaderField: "Authorization")
        request.setValue("Model Harbor", forHTTPHeaderField: "X-Title")
        let (bytes, response) = try await URLSession.shared.data(for: request)
        guard let status = (response as? HTTPURLResponse)?.statusCode else { throw ProviderError.message("OpenRouter could not be reached. Try again.") }
        guard status == 200 else {
            if status == 401 || status == 403 { throw ProviderError.message("OpenRouter did not accept this key. Check it and try again.") }
            throw ProviderError.message("OpenRouter returned HTTP \(status). Try again shortly.")
        }
        return bytes
    }
}
