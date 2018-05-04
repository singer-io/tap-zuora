# Changelog

## 1.0.2
  * Fixes a bug where the CSV reader can choke on CSV data containing null bytes [#21](https://github.com/singer-io/tap-zuora/pull/21)

## 1.0.1
  * Fixes bug where rest queries did not have a '.' between the name of a joined object and the field
  * Bumps version of singer-python to 5.1.0

## 1.0.0
  * Initial release to Stitch platform for production

## 0.3.0
  * Adds ability for timed-out AQuA requests to continue to request the export in subsequent runs through the state
  * Upgrade singer-python to 5.0.15

## 0.2.3
  * Bumps version of singer-python to 5.0.14 to fix datetime strftime issues documented in [#69](https://github.com/singer-io/singer-python/pull/69)

## 0.2.2
  * Changes the discovery behavior to all any field to have a null value - not just "required" fields [#17](https://github.com/singer-io/tap-zuora/pull/17)

## 0.2.1
 * Fixes 'related-objects' to use metadata so tap knows which fields need to have a '.' added during the query

## 0.2.0
  * Feature to use an object's `related-objects` data to add foreign keys to Zuora "Joined Objects" [#12](https://github.com/singer-io/tap-zuora/pull/12)
  * Bumps the version of singer-python to better support 2 digit date formatting [#13](https://github.com/singer-io/tap-zuora/pull/13)
  * Bumps the default timeout for Zuora Jobs from 60 minutes to 90 [#14](https://github.com/singer-io/tap-zuora/pull/14)

## 0.1.5
  * Fixes issue where non-discoverable stream threw an exception that caused the tap to crash [#11](https://github.com/singer-io/tap-zuora/pull/11)

## 0.1.4
  * Fixes bugs when trying to select deleted records from objects that do not support it and skips empty lines [#10](https://github.com/singer-io/tap-zuora/pull/10)

## 0.1.3
  * Adds and fixes some pylint [#9](https://github.com/singer-io/tap-zuora/pull/9)
  * Prevents errors when certain restricted objects attempt to query for deleted records [#8](https://github.com/singer-io/tap-zuora/pull/8)

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
