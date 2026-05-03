BEGIN;

CREATE TABLE IF NOT EXISTS ai_duplicate_feedbacks (
    id UUID PRIMARY KEY,
    prop_a_id UUID NOT NULL REFERENCES properties(id) ON DELETE CASCADE,
    prop_b_id UUID NOT NULL REFERENCES properties(id) ON DELETE CASCADE,
    hash_a VARCHAR(64) NOT NULL,
    hash_b VARCHAR(64) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL,
    CONSTRAINT uix_ai_feedback_pair UNIQUE (prop_a_id, prop_b_id)
);

CREATE INDEX IF NOT EXISTS idx_ai_feedback_a ON ai_duplicate_feedbacks(prop_a_id);
CREATE INDEX IF NOT EXISTS idx_ai_feedback_b ON ai_duplicate_feedbacks(prop_b_id);

COMMIT;