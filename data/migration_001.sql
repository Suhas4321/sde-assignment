-- Migration: Add processing_tasks, audit_log, customer_token_budgets and alter interactions table.

CREATE TYPE task_status AS ENUM ('pending', 'in_progress', 'completed', 'failed', 'dead_letter');
CREATE TYPE task_priority AS ENUM ('hot', 'cold', 'skip');

-- 1. Create customer_token_budgets table
CREATE TABLE customer_token_budgets (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    customer_id UUID UNIQUE NOT NULL,
    tokens_per_minute INTEGER NOT NULL,
    requests_per_minute INTEGER NOT NULL,
    priority VARCHAR(50) DEFAULT 'standard', -- "standard", "premium", "enterprise"
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_customer_token_budgets_customer ON customer_token_budgets(customer_id);

-- 2. Create processing_tasks table
CREATE TABLE processing_tasks (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    interaction_id UUID NOT NULL REFERENCES interactions(id) ON DELETE CASCADE,
    customer_id UUID NOT NULL,
    campaign_id UUID NOT NULL,
    task_type VARCHAR(100) NOT NULL, -- "llm_analysis", "recording_upload", "signal_jobs", "lead_update"
    priority task_priority DEFAULT 'cold',
    status task_status DEFAULT 'pending',
    attempt_count INTEGER DEFAULT 0,
    max_attempts INTEGER DEFAULT 5,
    payload JSONB DEFAULT '{}',
    result JSONB DEFAULT '{}',
    error_message TEXT,
    next_retry_at TIMESTAMPTZ,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_processing_tasks_interaction ON processing_tasks(interaction_id);
CREATE INDEX idx_processing_tasks_status ON processing_tasks(status);
CREATE INDEX idx_processing_tasks_next_retry ON processing_tasks(next_retry_at);

-- 3. Create audit_log table
CREATE TABLE audit_log (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    interaction_id UUID REFERENCES interactions(id) ON DELETE SET NULL,
    customer_id UUID NOT NULL,
    campaign_id UUID NOT NULL,
    event_type VARCHAR(100) NOT NULL,
    step VARCHAR(100) NOT NULL,
    status VARCHAR(50) NOT NULL, -- "started", "completed", "failed"
    attempt INTEGER DEFAULT 1,
    event_metadata JSONB DEFAULT '{}',
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_audit_log_interaction ON audit_log(interaction_id);
CREATE INDEX idx_audit_log_customer ON audit_log(customer_id);

-- 4. Alter interactions table
ALTER TABLE interactions ADD COLUMN processing_priority VARCHAR(50) DEFAULT 'cold';
ALTER TABLE interactions ADD COLUMN llm_tokens_used INTEGER DEFAULT 0;
ALTER TABLE interactions ADD COLUMN recording_status VARCHAR(50) DEFAULT 'pending';
ALTER TABLE interactions ADD COLUMN recording_attempts INTEGER DEFAULT 0;
ALTER TABLE interactions ADD COLUMN processing_started_at TIMESTAMPTZ;
ALTER TABLE interactions ADD COLUMN processing_completed_at TIMESTAMPTZ;

-- 5. Create customer_configs table
CREATE TABLE customer_configs (
    customer_id UUID PRIMARY KEY,
    hot_keywords VARCHAR(100)[] DEFAULT ARRAY['confirmed', 'booked', 'scheduled', 'appointment', 'manager', 'escalate', 'complaint', 'urgent', 'demo', 'meeting', 'canceled', 'refund'],
    crm_webhook_url VARCHAR(500),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

