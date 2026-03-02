terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

# Provider for demo2-service project
provider "google" {
  alias   = "demo2"
  project = var.demo2_project_id
  region  = var.region
  zone    = var.zone
}

# Provider for demo3-service project
provider "google" {
  alias   = "demo3"
  project = var.demo3_project_id
  region  = var.region
  zone    = var.zone
}

# ── demo2-service VPC ────────────────────────────────────────────────

resource "google_compute_network" "demo2_vpc" {
  provider                = google.demo2
  name                    = "billing-spend-vpc"
  auto_create_subnetworks = false
}

resource "google_compute_subnetwork" "demo2_subnet" {
  provider      = google.demo2
  name          = "billing-spend-subnet"
  ip_cidr_range = "10.0.0.0/24"
  region        = var.region
  network       = google_compute_network.demo2_vpc.id
}

resource "google_compute_firewall" "demo2_allow_internal" {
  provider = google.demo2
  name     = "billing-spend-allow-internal"
  network  = google_compute_network.demo2_vpc.id

  allow {
    protocol = "icmp"
  }
  allow {
    protocol = "tcp"
    ports    = ["0-65535"]
  }
  allow {
    protocol = "udp"
    ports    = ["0-65535"]
  }

  source_ranges = ["10.0.0.0/24"]
}

# ── demo2-service Compute Instance (~$0.388/hr = ~$4.66 over 12h) ───

resource "google_compute_instance" "demo2_billing" {
  provider     = google.demo2
  name         = "billing-spend-demo2"
  machine_type = "n2-standard-8"
  zone         = var.zone

  boot_disk {
    initialize_params {
      image = "debian-cloud/debian-12"
      size  = 10
      type  = "pd-standard"
    }
  }

  network_interface {
    subnetwork = google_compute_subnetwork.demo2_subnet.id
  }

  labels = {
    purpose = "billing-data-generation"
    cleanup = "terraform-destroy"
  }
}

# ── demo3-service VPC ────────────────────────────────────────────────

resource "google_compute_network" "demo3_vpc" {
  provider                = google.demo3
  name                    = "billing-spend-vpc"
  auto_create_subnetworks = false
}

resource "google_compute_subnetwork" "demo3_subnet" {
  provider      = google.demo3
  name          = "billing-spend-subnet"
  ip_cidr_range = "10.0.0.0/24"
  region        = var.region
  network       = google_compute_network.demo3_vpc.id
}

resource "google_compute_firewall" "demo3_allow_internal" {
  provider = google.demo3
  name     = "billing-spend-allow-internal"
  network  = google_compute_network.demo3_vpc.id

  allow {
    protocol = "icmp"
  }
  allow {
    protocol = "tcp"
    ports    = ["0-65535"]
  }
  allow {
    protocol = "udp"
    ports    = ["0-65535"]
  }

  source_ranges = ["10.0.0.0/24"]
}

# ── demo3-service Compute Instance (~$0.388/hr = ~$4.66 over 12h) ───

resource "google_compute_instance" "demo3_billing" {
  provider     = google.demo3
  name         = "billing-spend-demo3"
  machine_type = "n2-standard-8"
  zone         = var.zone

  boot_disk {
    initialize_params {
      image = "debian-cloud/debian-12"
      size  = 10
      type  = "pd-standard"
    }
  }

  network_interface {
    subnetwork = google_compute_subnetwork.demo3_subnet.id
  }

  labels = {
    purpose = "billing-data-generation"
    cleanup = "terraform-destroy"
  }
}
