-- ════════════════════════════════════════════════════════════════════
--  Sample Employee Database Schema + Seed Data
--  Run once against your PostgreSQL instance:
--    psql -U postgres -d employees -f scripts/seed_db.sql
-- ════════════════════════════════════════════════════════════════════

-- ── Tables ───────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS departments (
    id          SERIAL PRIMARY KEY,
    name        VARCHAR(100) NOT NULL UNIQUE,
    location    VARCHAR(100),
    budget      NUMERIC(15, 2)
);

CREATE TABLE IF NOT EXISTS employees (
    id              SERIAL PRIMARY KEY,
    name            VARCHAR(150) NOT NULL,
    email           VARCHAR(200) UNIQUE,
    department_id   INTEGER REFERENCES departments(id),
    job_title       VARCHAR(100),
    salary          NUMERIC(12, 2) NOT NULL,
    hire_date       DATE NOT NULL,
    is_active       BOOLEAN DEFAULT TRUE,
    manager_id      INTEGER REFERENCES employees(id)
);

CREATE TABLE IF NOT EXISTS performance_reviews (
    id              SERIAL PRIMARY KEY,
    employee_id     INTEGER NOT NULL REFERENCES employees(id),
    review_year     INTEGER NOT NULL,
    score           NUMERIC(3, 1) CHECK (score BETWEEN 1.0 AND 5.0),
    comments        TEXT,
    reviewed_at     TIMESTAMP DEFAULT NOW()
);

-- ── Seed data ─────────────────────────────────────────────────────────────────

INSERT INTO departments (name, location, budget) VALUES
    ('Engineering',      'New York',    2500000.00),
    ('Product',          'San Francisco', 1800000.00),
    ('Marketing',        'Chicago',     1200000.00),
    ('Human Resources',  'Austin',       900000.00),
    ('Finance',          'New York',    1100000.00),
    ('IT',               'Remote',      1600000.00),
    ('Operations',       'Dallas',       950000.00),
    ('Sales',            'Miami',       2000000.00)
ON CONFLICT DO NOTHING;

INSERT INTO employees (name, email, department_id, job_title, salary, hire_date, manager_id) VALUES
    ('Alice Johnson',   'alice@corp.com',   1, 'Senior Engineer',       145000, '2018-03-15', NULL),
    ('Bob Smith',       'bob@corp.com',     1, 'Junior Engineer',        85000, '2022-07-01', 1),
    ('Carol White',     'carol@corp.com',   1, 'Lead Engineer',         175000, '2015-11-20', 1),
    ('David Brown',     'david@corp.com',   2, 'Product Manager',       130000, '2019-05-10', NULL),
    ('Eve Martinez',    'eve@corp.com',     2, 'Junior PM',              78000, '2023-01-15', 4),
    ('Frank Wilson',    'frank@corp.com',   3, 'Marketing Director',    160000, '2017-08-22', NULL),
    ('Grace Lee',       'grace@corp.com',   3, 'Marketing Analyst',      72000, '2021-09-01', 6),
    ('Hank Taylor',     'hank@corp.com',    4, 'HR Manager',            110000, '2016-04-30', NULL),
    ('Irene Anderson',  'irene@corp.com',   4, 'HR Specialist',          65000, '2022-11-14', 8),
    ('Jack Thomas',     'jack@corp.com',    5, 'CFO',                   210000, '2013-01-05', NULL),
    ('Karen Jackson',   'karen@corp.com',   5, 'Finance Analyst',        88000, '2020-06-20', 10),
    ('Leo Harris',      'leo@corp.com',     6, 'IT Director',           155000, '2016-12-01', NULL),
    ('Mia Clark',       'mia@corp.com',     6, 'Systems Administrator',  95000, '2019-03-18', 12),
    ('Noah Lewis',      'noah@corp.com',    6, 'DevOps Engineer',       120000, '2021-07-07', 12),
    ('Olivia Robinson', 'olivia@corp.com',  7, 'Operations Manager',    125000, '2018-10-25', NULL),
    ('Paul Walker',     'paul@corp.com',    7, 'Logistics Coordinator',  68000, '2023-04-01', 15),
    ('Quinn Hall',      'quinn@corp.com',   8, 'Sales Director',        180000, '2014-09-14', NULL),
    ('Rachel Young',    'rachel@corp.com',  8, 'Sales Executive',       105000, '2020-02-28', 17),
    ('Sam King',        'sam@corp.com',     8, 'Sales Executive',        98000, '2021-05-11', 17),
    ('Tina Scott',      'tina@corp.com',    1, 'Staff Engineer',        165000, '2016-06-03', 3)
ON CONFLICT DO NOTHING;

INSERT INTO performance_reviews (employee_id, review_year, score, comments) VALUES
    (1,  2023, 4.5, 'Excellent technical leadership'),
    (2,  2023, 3.8, 'Good progress, needs more independence'),
    (3,  2023, 4.9, 'Outstanding performance, mentor to team'),
    (4,  2023, 4.2, 'Strong delivery, cross-functional collaborator'),
    (5,  2023, 3.5, 'Shows promise, still ramping up'),
    (10, 2023, 4.7, 'Exceptional financial strategy'),
    (12, 2023, 4.4, 'Great IT leadership and modernisation efforts'),
    (17, 2023, 4.6, 'Exceeded sales targets by 30%'),
    (20, 2023, 4.8, 'Technical excellence and mentorship')
ON CONFLICT DO NOTHING;
