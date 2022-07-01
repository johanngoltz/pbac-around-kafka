variable "subnetwork" {
  type = object({
    ip_cidr_range = string
    name = string
  })
}