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
                
                # CONFIRMATION KEYWORDS (The Fix)
                # These ensure 'yes' keeps us in Mode C
                "yes", "yep", "yeah", "sure", "ok", "okay", "correct", 
                "proceed", "go ahead", "continue", "right", "fine", "1", "2"
            ]
        }

    def classify_intent(self, user_input: str):
        """
        Determines the intent based on keyword matching.
        Defaults to 'data_engineer' (Mode C) for ambiguous inputs like 'yes'.
        """
        user_input = user_input.lower().strip()
        
        # Check specific routes
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
        
        # If no keywords matched (score 0), default to Engineer (Mode C)
        # because Mode C handles general conversation/follow-ups best.
        if scores[best_intent] == 0:
            return "data_engineer"
            
        return best_intent

    def get_workflow_description(self, intent):
        if intent == "audit_only":
            return "🔍 **AUDIT MODE**: I will scan your data for errors (Read-Only)."
        elif intent == "clean_data":
            return "🔧 **SURGEON MODE**: I will fix issues interactively."
        else:
            return "🏗️ **ENGINEER MODE**: Pipeline activated (Clean -> Model -> Query)."

router = Router()