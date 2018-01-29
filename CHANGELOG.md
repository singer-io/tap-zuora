# Changelog

## 0.1.2
  * Fixes a bug with AQuA queries being created incorrectly when deleted objects are desired [#7](https://github.com/singer-io/tap-zuora/pull/7)

## 0.1.1
  * Fixes a bug with accesing partner_id as the Client is being created [#4](https://github.com/singer-io/tap-zuora/pull/4)
  * Output the Zuora query to the log for visibility [#5](https://github.com/singer-io/tap-zuora/pull/5)
  * Adds Singer metadata to the Deleted field added for Zuora objects that support it [#6](https://github.com/singer-io/tap-zuora/pull/6)

## 0.1.0
  * Large rework of the Zuora Tap [#3](https://github.com/singer-io/tap-zuora/pull/3)
    * Rework the tap to support both the AQuA and REST API's.
    * Allows the streaming of deleted Zuora data using the AQuA API.
    * Adds support for Zuora Sandbox accounts and European accounts.
