# ========================================
# GLUE ICEBERG TABLES VARIABLES
# ========================================
# Additional variables for Glue-managed Iceberg tables
# ========================================

variable "iceberg_table_prefix" {
  description = "Prefix for Iceberg table S3 paths"
  type        = string
  default     = "iceberg-performance-test/iceberg_glue_format"
}

variable "iceberg_compression_codec" {
  description = "Compression codec for Iceberg tables"
  type        = string
  default     = "snappy"
  validation {
    condition     = contains(["snappy", "gzip", "lz4", "zstd"], var.iceberg_compression_codec)
    error_message = "Compression codec must be one of: snappy, gzip, lz4, zstd."
  }
}

variable "iceberg_target_file_size_bytes" {
  description = "Target file size in bytes for Iceberg tables"
  type        = number
  default     = 134217728 # 128MB
  validation {
    condition     = var.iceberg_target_file_size_bytes > 0
    error_message = "Target file size must be greater than 0."
  }
}

variable "iceberg_distribution_mode" {
  description = "Distribution mode for Iceberg tables"
  type        = string
  default     = "hash"
  validation {
    condition     = contains(["hash", "range", "none"], var.iceberg_distribution_mode)
    error_message = "Distribution mode must be one of: hash, range, none."
  }
}

variable "iceberg_write_format" {
  description = "Write format for Iceberg tables"
  type        = string
  default     = "parquet"
  validation {
    condition     = contains(["parquet", "orc", "avro"], var.iceberg_write_format)
    error_message = "Write format must be one of: parquet, orc, avro."
  }
}

variable "enable_iceberg_optimization" {
  description = "Enable Iceberg table optimization features"
  type        = bool
  default     = true
}

variable "iceberg_optimization_schedule" {
  description = "Schedule for Iceberg table optimization (cron expression)"
  type        = string
  default     = "0 2 * * *" # Daily at 2 AM
}

variable "enable_iceberg_compaction" {
  description = "Enable automatic Iceberg table compaction"
  type        = bool
  default     = true
}

variable "iceberg_compaction_threshold" {
  description = "Number of small files before triggering compaction"
  type        = number
  default     = 5
  validation {
    condition     = var.iceberg_compaction_threshold > 0
    error_message = "Compaction threshold must be greater than 0."
  }
}

variable "iceberg_table_retention_days" {
  description = "Number of days to retain Iceberg table snapshots"
  type        = number
  default     = 30
  validation {
    condition     = var.iceberg_table_retention_days > 0
    error_message = "Table retention days must be greater than 0."
  }
}

variable "enable_iceberg_time_travel" {
  description = "Enable time travel for Iceberg tables"
  type        = bool
  default     = true
}

variable "iceberg_time_travel_days" {
  description = "Number of days for time travel retention"
  type        = number
  default     = 7
  validation {
    condition     = var.iceberg_time_travel_days > 0
    error_message = "Time travel days must be greater than 0."
  }
}

variable "iceberg_table_comment" {
  description = "Default comment for Iceberg tables"
  type        = string
  default     = "TPC-DS table managed by AWS Glue with Iceberg format"
}

variable "enable_iceberg_metrics" {
  description = "Enable Iceberg table metrics collection"
  type        = bool
  default     = true
}

variable "iceberg_metrics_retention_days" {
  description = "Number of days to retain Iceberg metrics"
  type        = number
  default     = 90
  validation {
    condition     = var.iceberg_metrics_retention_days > 0
    error_message = "Metrics retention days must be greater than 0."
  }
}
