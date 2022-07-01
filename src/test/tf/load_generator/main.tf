terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = ">= 4"
    }
  }
}

module "gce-container" {
  source  = "terraform-google-modules/container-vm/google"
  version = "~> 3.0"

  container = {
    image   = "docker.io/confluentinc/cp-kafka:7.1.1"
    command = [
      "/bin/sh",
      "-c",
      "nc --listen 8080 && /usr/bin/kafka-producer-perf-test"
    ]
  }

  restart_policy = "never"
}

resource "google_compute_instance" "default" {
  count = var.client_count

  name         = "${var.client_type}-${count.index}"
  machine_type = "e2-small"

  boot_disk {
    initialize_params {
      image = module.gce-container.source_image
    }
  }

  network_interface {
    network    = var.network_name
    subnetwork = "clients-subnetwork"
    # access_config {}
  }

  tags = [var.client_type, "benchmark"]

  metadata = {
    serial-port-logging-enable = "TRUE"
    gce-container-declaration  = module.gce-container.metadata_value
    google-logging-enabled     = "true"
    google-monitoring-enabled  = "true"
  }

  labels = {
    container-vm = module.gce-container.vm_container_label
  }

  service_account {
    email  = "terraform-local@pbac-in-pubsub.iam.gserviceaccount.com"
    scopes = ["cloud-platform"]
  }
}