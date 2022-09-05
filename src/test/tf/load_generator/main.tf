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
    image   = "us-central1-docker.pkg.dev/pbac-in-pubsub/the-repo/benchmark-load-generator:latest"
    command = [
      "/usr/bin/run-${var.client_type}-bench.sh"
    ]
    env = [
      {
        name  = "BENCHMARK_NUM_RECORDS"
        value = var.msg_count
      },
      {
        name = "BENCH_NEW_RESERVATION_FREQUENCY_SECONDS"
        value = var.new_reservation_interval
      },
      {
        name = "BENCH_NUM_DUMMY_RESERVATIONS"
        value = "100000"
      }
    ]
  }

  restart_policy = "Never"
}

resource "google_compute_instance" "default" {
  count = var.client_count

  name         = "${var.client_type}-${count.index}"
  machine_type = "t2d-standard-1"

  boot_disk {
    initialize_params {
      image = module.gce-container.source_image
      size  = 10
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