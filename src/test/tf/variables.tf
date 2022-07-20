variable "client_count" {
  type    = number
  default = 4
  validation {
    condition     = var.client_count > 0
    error_message = "must be positive"
  }
}
variable "client_types" {
  type = string
  #  validation {
  #    condition     = setsubtract(var.client_types, toset(["producer", "consumer"])) == []
  #    error_message = "only producer, consumer are allowed"
  #  }
}

variable "benchmark_msgs_to_send" {
  type    = number
  default = 50000000
}

variable "enable_pbac" {
  type = bool
}