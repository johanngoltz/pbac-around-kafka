terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = ">= 4"
    }
  }
}

locals {
  # todo migrate to DNS names?
  kafka_hosts = [for i in range(1) : cidrhost(var.subnetwork.ip_cidr_range, i + 3)]
  kafka_env   = [
    {
      name  = "KAFKA_CFG_ZOOKEEPER_CONNECT"
      value = "zookeeper:2181"
    },
    {
      name  = "KAFKA_CFG_ZOOKEEPER_CONNECTION_TIMEOUT_MS"
      value = "150000"
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
    }, {
      name  = "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR"
      value = "1"
    }
  ]
  pbac_env = [
    {
      name  = "PBAC_KAFKA_HOST"
      value = "kafka-0"
    },
    {
      name  = "PBAC_KAFKA_PORT"
      value = "9092"
    },
    {
      name  = "PBAC_NUM_THREADS"
      value = "100"
    },
    {
      name  = "PBAC_MODE"
      value = "FILTER_ON_PUBLISH"
    },
    {
      name  = "KAFKA_CFG_LOG4J_ROOT_LOGLEVEL"
      value = "TRACE"
    },
    {
      name  = "KAFKA_CFG_LOG4J_LOGGERS"
      value = "kafka=TRACE,kafka.request.logger=TRACE,org.apache.kafka=TRACE"
    }
  ]
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
  machine_type = "n2d-standard-2"

  boot_disk {
    initialize_params {
      image = "cos-cloud/cos-stable"
      size  = 100
      type  = "pd-ssd"
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
    user-data                  = <<EOT
      #cloud-config

      users:
      - name: cloudservice
        uid: 2000

      write_files:
      - path: /etc/systemd/system/config-firewall.service
        permissions: 0644
        owner: root
        content: |
          [Unit]
          Description=Configures the host firewall

          [Service]
          Type=oneshot
          RemainAfterExit=true
          ExecStart=/sbin/iptables -A INPUT -p tcp --match multiport --dports 9092,9093,29092 -j ACCEPT
      - path: /etc/systemd/system/kafka.service
        permissions: 0644
        owner: root
        content: |
          [Unit]
          Description=Kafka broker
          Wants=gcr-online.target
          After=gcr-online.target, config-firewall.service

          [Service]
          Environment="HOME=/home/cloudservice"
          ExecStart=/usr/bin/docker run -u 2000 --name=kafka --network host --env KAFKA_CFG_ADVERTISED_LISTENERS=CLIENT://kafka-${count.index}:29092,EXTERNAL://kafka-${count.index}:%{if var.enable_pbac}9093%{else}9092%{endif} %{ for envvar in local.kafka_env} --env ${envvar.name}=${envvar.value} %{ endfor } docker.io/bitnami/kafka:3.1
          ExecStop=/usr/bin/docker stop kafka
          ExecStopPost=/usr/bin/docker rm kafka
      - path: /etc/systemd/system/pbac.service
        permissions: 0644
        owner: root
        content: |
          [Unit]
          Description=PBAC
          After=kafka.service

          [Service]
          Environment="HOME=/home/cloudservice"
          ExecStartPre=/usr/bin/docker-credential-gcr configure-docker --registries us-central1-docker.pkg.dev
          ExecStart=/usr/bin/docker run -u 2000 --name=pbac --network host %{ for envvar in local.pbac_env} --env ${envvar.name}=${envvar.value} %{ endfor } us-central1-docker.pkg.dev/pbac-in-pubsub/the-repo/pbac-around-kafka:latest
          ExecStop=/usr/bin/docker stop pbac
          ExecStopPost=/usr/bin/docker rm pbac

      runcmd:
      - systemctl start config-firewall.service
      - echo 'DOCKER_OPTS="--registry-mirror=https://mirror.gcr.io"' | tee /etc/default/docker
      - systemctl daemon-reload
      - systemctl restart docker
      - systemctl start kafka.service
      %{ if var.enable_pbac } - systemctl start pbac.service %{ endif }
    EOT
    google-logging-enabled     = "true"
    google-monitoring-enabled  = "true"
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