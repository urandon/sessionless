terraform {
  required_version = "= 1.15.5"

  required_providers {
    yandex = {
      source  = "yandex-cloud/yandex"
      version = "= 0.220.0"
    }
  }

  backend "s3" {
    endpoints = {
      s3 = "https://storage.yandexcloud.net"
    }

    region                      = "ru-central1"
    skip_credentials_validation = true
    skip_region_validation      = true
    skip_requesting_account_id  = true
    skip_s3_checksum            = true
    use_path_style              = true
    use_lockfile                = true
  }
}

provider "yandex" {
  cloud_id  = var.cloud_id
  folder_id = var.management_folder_id
  zone      = var.zone
}
