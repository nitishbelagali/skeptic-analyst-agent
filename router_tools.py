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
                "correct", "patch", "0", "auto-pilot"
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
                # Confirmation & Transitions
                "yes", "yep", "sure", "ok", "proceed", "go ahead", "confirm",
                "done", "finish", "finished", "completed", "next",
                "1", "2", "3"
            ]
        }

    def classify_intent(self, user_input: str):
        """
        Determines intent using strict shortcuts first, then fuzzy matching.
        """
        user_input = user_input.lower().strip()
        
        # --- 1. STRICT SHORTCUTS ---
        
        # "0" is exclusively the Auto-Pilot Cleaning option
        if user_input == "0": 
            return "clean_data"
        
        # "1", "2", "3" are menu selections (usually Analysis/Engineering choices)
        # 1 = Download / Deep Dive
        # 2 = ER Model / Dashboard
        # 3 = Custom SQL
        if user_input in ["1", "2", "3"]:
            return "data_engineer"
            
        # "Done" signals the end of a cleaning loop -> Move to Engineering options
        if user_input in ["done", "finish", "finished", "next"]:
            return "data_engineer"
        
        # --- 2. FUZZY MATCH ---
        best_score = 0
        best_intent = "data_engineer" # Default fallback
        
        for intent, keywords in self.routes.items():
            match, score = process.extractOne(user_input, keywords)
            if score > 80 and score > best_score:
                best_score = score
                best_intent = intent
        
        # --- 3. CONTEXT FALLBACKS ---
        # Specific overrides if fuzzy matching was borderline
        if "clean" in user_input or "fix" in user_input: return "clean_data"
        if "audit" in user_input: return "audit_only"
            
        return best_intent

    def get_workflow_description(self, intent):
        if intent == "audit_only":
            return "🔍 **AUDIT MODE**: Scanning for anomalies..."
        elif intent == "clean_data":
            return "🔧 **SURGEON MODE**: Ready to repair data."
        else:
            return "🏗️ **ENGINEER MODE**: Pipeline activated."

router = Router()