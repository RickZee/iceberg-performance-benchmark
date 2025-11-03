# ========================================
# TABLE LIST FOR TERRAFORM
# Snowflake Iceberg Performance Testing Project
# ========================================

# Normalized Tables (20)
locals {
  normalized_tables = [
    "addresses",
    "borrowers", 
    "properties",
    "servicers",
    "loans",
    "payments",
    "credit_scores",
    "borrower_contacts",
    "employment_history",
    "insurance_policies",
    "loan_audit_log",
    "loan_balances",
    "loan_documents",
    "loan_insurance",
    "loan_modifications",
    "loan_servicing_notes",
    "payment_schedules",
    "property_appraisals",
    "property_taxes",
    "transactions"
  ]

  # Denormalized Tables (0)
  denormalized_tables = []

  # All Tables (20)
  all_tables = concat(local.normalized_tables, local.denormalized_tables)
}
