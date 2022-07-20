terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 4"
    }
  }
}

variable "project_id" {
  default = "pbac-in-pubsub"
}

provider "google" {
  credentials = file("pbac-in-pubsub-40990a3f591d.json")

  project = var.project_id
  region  = "us-central1"
  zone    = "us-central1-f"
}

provider "google-beta" {
  credentials = file("pbac-in-pubsub-40990a3f591d.json")

  project = var.project_id
  region  = "us-central1"
  zone    = "us-central1-f"
}

resource "google_compute_network" "default" {
  name                    = "terraform-network"
  auto_create_subnetworks = false
}

resource "google_compute_subnetwork" "kafka" {
  ip_cidr_range = "10.128.0.0/24"
  name          = "kafka-subnetwork"
  network       = google_compute_network.default.name
}

resource "google_compute_subnetwork" "clients" {
  ip_cidr_range = "10.128.1.0/24"
  name          = "clients-subnetwork"
  network       = google_compute_network.default.name
}

resource "google_compute_firewall" "default" {
  name    = "terraform-firewall"
  network = google_compute_network.default.name

  allow {
    protocol = "icmp"
  }

  allow {
    protocol = "tcp"
    ports    = ["22", "80", "8080", "2181", "9090-9093", "29092"]
  }

  source_ranges = ["0.0.0.0/0"]
}

module "cloud_router" {
  source  = "terraform-google-modules/cloud-router/google"
  version = "~> 2"
  project = var.project_id
  name    = "my-cloud-router"
  network = google_compute_network.default.name
  region  = "us-central1"

  # The load generators don't get external IP addresses. Without either them or NAT, they cannot reach the container
  # registry to download the images.
  nats = [
    {
      name = "my-nat-gateway"
    }
  ]
}

module "kafka" {
  source    = "./kafka"
  providers = {
    google = google
  }

  enable_pbac = var.enable_pbac
  subnetwork = google_compute_subnetwork.kafka
}

module "load_generator" {
  for_each = toset(split(",", var.client_types))

  source    = "./load_generator"
  providers = {
    google = google
  }
  client_count = var.client_count
  client_type  = each.key
  network_name = google_compute_network.default.name
  msg_count    = var.benchmark_msgs_to_send
}

resource "google_compute_instance" "controller" {
  machine_type = "e2-small"
  name         = "controller"

  boot_disk {
    auto_delete = true
    initialize_params {
      image = "debian-cloud/debian-11"
    }
  }

  network_interface {
    subnetwork = google_compute_subnetwork.clients.name
    access_config {
      network_tier = "STANDARD"
    }
  }

  metadata_startup_script = "sudo apt-get update && sudo apt-get install -y default-jre wget ncat && wget https://dlcdn.apache.org/kafka/3.2.0/kafka_2.13-3.2.0.tgz && tar -xzf kafka_2.13-3.2.0.tgz -C / && rm kafka_2.13-3.2.0.tgz"

  service_account {
    # Google recommends custom service accounts that have cloud-platform scope and permissions granted via IAM Roles.
    email  = "terraform-local@pbac-in-pubsub.iam.gserviceaccount.com"
    scopes = ["cloud-platform"]
  }
}
