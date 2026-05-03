BEGIN;

CREATE TABLE IF NOT EXISTS scraper_logs (
    id UUID PRIMARY KEY,
    source_domain VARCHAR(100) NOT NULL,
    status VARCHAR(50) NOT NULL,
    processed_count INTEGER DEFAULT 0,
    new_count INTEGER DEFAULT 0,
    duration_seconds INTEGER,
    error_message TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_scraper_logs_domain ON scraper_logs(source_domain);
CREATE INDEX IF NOT EXISTS idx_scraper_logs_created_at ON scraper_logs(created_at);

CREATE TABLE IF NOT EXISTS email_logs (
    id UUID PRIMARY KEY,
    recipient_email VARCHAR(255) NOT NULL,
    status VARCHAR(50) NOT NULL,
    properties_count INTEGER DEFAULT 0,
    error_message TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_email_logs_recipient ON email_logs(recipient_email);
CREATE INDEX IF NOT EXISTS idx_email_logs_created_at ON email_logs(created_at);

COMMIT;