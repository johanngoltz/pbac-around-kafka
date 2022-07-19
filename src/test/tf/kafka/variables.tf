variable "subnetwork" {
  type = object({
    ip_cidr_range = string
    name = string
  })
}

variable "enable_pbac" {
  type = bool
  default = false
}