output "control_container_ids" { value = { for slot, container in yandex_serverless_container.control : slot => container.id } }
output "control_container_urls" { value = { for slot, container in yandex_serverless_container.control : slot => container.url } }
output "worker_container_id" { value = yandex_serverless_container.worker.id }
output "reconciler_container_id" { value = yandex_serverless_container.reconciler.id }
output "telegram_sender_container_id" { value = yandex_serverless_container.telegram_sender.id }
output "registry_gc_container_inventory" {
  value = merge(
    {
      for slot, container in yandex_serverless_container.control : "control-${slot}" => {
        container_id = container.id
        revision_id  = container.revision_id
        component    = "control-api"
        repository   = "control-api"
        slot         = slot
        source_sha   = var.image_source_shas["control-${slot}"]
        image_ref    = var.images["control-${slot}"]
      }
    },
    {
      reconciler = {
        container_id = yandex_serverless_container.reconciler.id
        revision_id  = yandex_serverless_container.reconciler.revision_id
        component    = "reconciler"
        repository   = "reconciler"
        slot         = "singleton"
        source_sha   = var.image_source_shas["reconciler"]
        image_ref    = var.images["reconciler"]
      }
      worker-runtime = {
        container_id = yandex_serverless_container.worker.id
        revision_id  = yandex_serverless_container.worker.revision_id
        component    = "worker-runtime"
        repository   = "worker-runtime"
        slot         = "singleton"
        source_sha   = var.image_source_shas["worker-runtime"]
        image_ref    = var.images["worker-runtime"]
      }
      telegram-sender = {
        container_id = yandex_serverless_container.telegram_sender.id
        revision_id  = yandex_serverless_container.telegram_sender.revision_id
        component    = "telegram-sender"
        repository   = "telegram-sender"
        slot         = "singleton"
        source_sha   = var.image_source_shas["telegram-sender"]
        image_ref    = var.images["telegram-sender"]
      }
    }
  )
}
