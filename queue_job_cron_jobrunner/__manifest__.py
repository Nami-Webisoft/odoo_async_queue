{
    "name": "Queue Job Cron Jobrunner",
    "summary": "Run jobs without a dedicated JobRunner",
    "description": "Odoo version 19 of fork of OCA/queue. Edits made by me (Nami)",
    "version": "19.0.1.1.0",
    "development_status": "Alpha",
    "author": "Nami & Camptocamp SA, Odoo Community Association (OCA)",
    "maintainers": ["ivantodorovich"],
    "website": "https://github.com/OCA/queue",
    "license": "AGPL-3",
    "category": "Others",
    "depends": ["queue_job"],
    "data": [
        "data/ir_cron.xml",
        "views/ir_cron.xml",
    ],
    "installable": True,
}
