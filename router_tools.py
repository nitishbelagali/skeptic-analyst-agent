class Router:
    def __init__(self):
        self.routes = {
            "audit_only": [
                "audit", "check", "verify", "errors", "issues", "quality", 
                "validate", "scan", "health", "debug"
            ],
            "clean_data": [
                "clean", "fix", "repair", "scrub", "remove", "fill", 
                "drop", "replace", "impute", "modify", "change", "sanitize"
            ],
            "data_engineer": [
                # Core analysis keywords
                "analyze", "analysis", "engineer", "pipeline", "warehouse", 
                "transform", "schema", "model", "star", "dimension", "fact",
                "dashboard", "visualize", "chart", "graph", "plot", "story",
                "trend", "pattern", "correlation", "kpi", "sql", "query",
                
                # Question words
                "which", "what", "how", "when", "where", "who",
                "find", "get", "show", "give", "calculate", "compare",
                
                # CONFIRMATION KEYWORDS (Critical for flow)
                # NOTE: Single digits are only Mode C if they appear ALONE
                "yes", "yep", "yeah", "sure", "ok", "okay", "correct", 
                "proceed", "go ahead", "continue", "right", "fine"
            ]
        }

    def classify_intent(self, user_input: str):
        """
        Determines intent based on keyword matching.
        Defaults to 'data_engineer' (Mode C) for ambiguous inputs.
        """
        user_input_original = user_input.strip()
        user_input = user_input.lower().strip()
        
        # SPECIAL CASE: Single digit alone (1-9) in Mode C context
        # If input is ONLY "1" or "2", keep it as data_engineer (fork choice)
        # If input is "1 median" or "3 cap", this is Mode B (cleaning option)
        if user_input_original in ["1", "2"]:
            return "data_engineer"  # Fork choice in Mode C
        
        # Score each route
        scores = {k: 0 for k in self.routes}
        
        for intent, keywords in self.routes.items():
            for word in keywords:
                if word in user_input:
                    scores[intent] += 1
        
        # If 'clean' and 'audit' tie, prefer 'clean'
        if scores["clean_data"] > 0 and scores["clean_data"] == scores["audit_only"]:
            return "clean_data"
        
        # Get best match
        best_intent = max(scores, key=scores.get)
        
        # If no keywords matched (score 0), default to data_engineer
        if scores[best_intent] == 0:
            return "data_engineer"
        
        return best_intent

    def get_workflow_description(self, intent):
        """Returns user-friendly description of selected mode."""
        if intent == "audit_only":
            return "🔍 **AUDIT MODE**: I will scan your data for errors (Read-Only)."
        elif intent == "clean_data":
            return "🔧 **SURGEON MODE**: I will fix issues interactively."
        else:
            return "🏗️ **ENGINEER MODE**: Pipeline activated (Clean → Model → Query)."

# Global router instance
router = Router()