terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = ">= 4"
    }
  }
}

locals {
  kafka_hosts = [for i in range(3) : cidrhost(var.subnetwork.ip_cidr_range, i + 3)]
}

module "gce-container-kafka" {
  count   = length(local.kafka_hosts)
  source  = "terraform-google-modules/container-vm/google"
  version = "~> 3.0"

  container = {
    image = "docker.io/bitnami/kafka:3.1"
    env   = [
      {
        name  = "KAFKA_CFG_ZOOKEEPER_CONNECT"
        value = "${google_compute_instance.zookeeper.network_interface[0].network_ip}:2181"
      },
      {
        name  = "ALLOW_PLAINTEXT_LISTENER"
        value = "yes"
      },
      {
        name  = "KAFKA_CFG_LISTENERS"
        value = "CLIENT://:29092,EXTERNAL://:9092"
      },
      {
        name  = "KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP"
        value = "CLIENT:PLAINTEXT,EXTERNAL:PLAINTEXT"
      },
      {
        name  = "KAFKA_CFG_ADVERTISED_LISTENERS"
        value = "CLIENT://${local.kafka_hosts[count.index]}:29092,EXTERNAL://${local.kafka_hosts[count.index]}:9092"

      },
      {
        name  = "KAFKA_CFG_INTER_BROKER_LISTENER_NAME"
        value = "CLIENT"
      },
      {
        name  = "KAFKA_CFG_LOG4J_ROOT_LOGLEVEL"
        value = "TRACE"
      },
      {
        name  = "KAFKA_CFG_LOG4J_LOGGERS"
        value = "kafka=TRACE,kafka.request.logger=TRACE,org.apache.kafka=TRACE"
      }
      #      ,{
      #        name  = "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR"
      #        value = "1"
      #      }
    ]
  }

  restart_policy = "never"
}

module "gce-container-zookeeper" {
  source  = "terraform-google-modules/container-vm/google"
  version = "~> 3.0"

  container = {
    image = "docker.io/bitnami/zookeeper:3.8"
    env   = [
      {
        name  = "ALLOW_ANONYMOUS_LOGIN"
        value = "yes"
      }
    ]
  }

  restart_policy = "never"
}

resource "google_compute_instance" "kafka" {
  count = length(local.kafka_hosts)

  name         = "kafka-${count.index}"
  machine_type = "e2-standard-2"

  boot_disk {
    initialize_params {
      image = module.gce-container-kafka[count.index].source_image
      size = 100
    }
  }

  network_interface {
    subnetwork = var.subnetwork.name
    network_ip = local.kafka_hosts[count.index]
    # access_config {}
  }

  tags = ["kafka-broker", "benchmark"]

  metadata = {
    serial-port-logging-enable = "TRUE"
    gce-container-declaration  = module.gce-container-kafka[count.index].metadata_value
    google-logging-enabled     = "true"
    google-monitoring-enabled  = "true"
  }

  labels = {
    container-vm = module.gce-container-kafka[count.index].vm_container_label
  }

  service_account {
    email  = "terraform-local@pbac-in-pubsub.iam.gserviceaccount.com"
    scopes = ["cloud-platform"]
  }
}

resource "google_compute_instance" "zookeeper" {
  name         = "zookeeper"
  machine_type = "e2-micro"

  boot_disk {
    initialize_params {
      image = module.gce-container-zookeeper.source_image
    }
  }

  network_interface {
    subnetwork = var.subnetwork.name
    network_ip = cidrhost(var.subnetwork.ip_cidr_range, 2)
    # access_config {}
  }

  tags = ["zookeeper", "benchmark"]

  metadata = {
    serial-port-logging-enable = "TRUE"
    gce-container-declaration  = module.gce-container-zookeeper.metadata_value
    google-logging-enabled     = "true"
    google-monitoring-enabled  = "true"
  }

  labels = {
    container-vm = module.gce-container-zookeeper.vm_container_label
  }

  service_account {
    email  = "terraform-local@pbac-in-pubsub.iam.gserviceaccount.com"
    scopes = ["cloud-platform"]
  }
}