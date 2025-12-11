from thefuzz import process

class Router:
    def __init__(self):
        self.routes = {
            "audit_only": [
                "audit", "check", "verify", "errors", "issues", "quality", 
                "validate", "scan", "health", "debug", "sanity"
            ],
            "clean_data": [
                "clean", "fix", "repair", "scrub", "remove", "fill", 
                "drop", "replace", "impute", "modify", "change", "sanitize",
                "correct", "patch"
            ],
            "data_engineer": [
                # Analysis
                "analyze", "analysis", "engineer", "pipeline", "warehouse", 
                "transform", "schema", "model", "star", "dimension", "fact",
                "dashboard", "visualize", "chart", "graph", "plot", "story",
                "trend", "pattern", "correlation", "kpi", "sql", "query",
                # Discovery
                "which", "what", "how", "when", "where", "who",
                "find", "get", "show", "give", "calculate", "compare",
                # Confirmation
                "yes", "yep", "sure", "ok", "proceed", "go ahead"
            ]
        }

    def classify_intent(self, user_input: str):
        """
        Determines intent using Fuzzy Matching (Score > 80).
        """
        user_input = user_input.lower().strip()
        
        # 1. Exact Match Shortcuts
        if user_input in ["1", "2"]: return "data_engineer"
        
        # 2. Fuzzy Match against all keyword lists
        best_score = 0
        best_intent = "data_engineer" # Default
        
        for intent, keywords in self.routes.items():
            # Extract the best matching keyword from this intent's list
            match, score = process.extractOne(user_input, keywords)
            if score > best_score:
                best_score = score
                best_intent = intent
        
        # Threshold check (80% confidence)
        if best_score > 80:
            return best_intent
            
        # Fallback Logic: Context clues
        if "clean" in user_input or "fix" in user_input: return "clean_data"
        if "audit" in user_input or "check" in user_input: return "audit_only"
            
        return "data_engineer"

    def get_workflow_description(self, intent):
        if intent == "audit_only":
            return "🔍 **AUDIT MODE**: Scanning for anomalies..."
        elif intent == "clean_data":
            return "🔧 **SURGEON MODE**: Ready to repair data."
        else:
            return "🏗️ **ENGINEER MODE**: Pipeline activated."

router = Router()