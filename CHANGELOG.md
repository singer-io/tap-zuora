# Changelog

## 1.3.0
  * Support endpoints for Zuora data centers that were initialized after September 2020 [#58](https://github.com/singer-io/tap-zuora/pull/58)

## 1.2.4
  * Change datetimes in REST queries to explicitly use UTC [#56](https://github.com/singer-io/tap-zuora/pull/56)

## 1.2.3
  * Handle non-rectangular CSV files by getting a new stateful AQuA session on the next sync [#54](https://github.com/singer-io/tap-zuora/pull/54)

## 1.2.2
  * Delete AQuA API discovery exports when they are no longer needed, to reduce concurrency of jobs. [#53](https://github.com/singer-io/tap-zuora/pull/53)
  * Add exponential backoff to AQuA requests for 429 errors [#53](https://github.com/singer-io/tap-zuora/pull/53)

## 1.2.1
  * Fixed REST queries to use iso-8601 timestamp [#50](https://github.com/singer-io/tap-zuora/pull/50)

## 1.2.0
  * Syncs soft deleted records [#48](https://github.com/singer-io/tap-zuora/pull/48)

## 1.1.12
  * Remove `incremental_time` from aqua queries [#34](https://github.com/singer-io/tap-zuora/pull/34)

## 1.1.11
 * Add http request metrics

## 1.1.10
 * TODO

## 1.1.9
 * Add `time_extracted` to record messages [commit](https://github.com/singer-io/tap-zuora/commit/91c602f488dddd07ef6d205dc3507fd7713e5f05)

## 1.1.8
 * Add timezone to ZOQL AQuA export queries [#37](https://github.com/singer-io/tap-zuora/pull/37)

## 1.1.7
  * Use T delimited date format for ZOQL AQuA export queries [#35](https://github.com/singer-io/tap-zuora/pull/35)

## 1.1.6
  * Detect non-rectangular CSV exports and error if found [#33](https://github.com/singer-io/tap-zuora/pull/33)

## 1.1.5
  * Log information about the details of an AQuA export to provide insight into `deleted` record behavior at a glance

## 1.1.4
  * Updates the tuple access of the AQUA endpoint switch [#31](https://github.com/singer-io/tap-zuora/pull/31)

## 1.1.3
  * Update version of `requests` to `2.20.0` in response to CVE 2018-18074

## 1.1.2
  * Whether the Zuora instance is a sandbox or a European instance affects URL paths for AQUA [#30](https://github.com/singer-io/tap-zuora/pull/30)

## 1.1.1
  * When resuming sync from `file_ids` in the state, there's a chance that a file could be deleted. If this occurs, the tap will remove the `file_ids` state value and resume from the bookmark on the next run to ensure a full window of data [#29](https://github.com/singer-io/tap-zuora/pull/29)

## 1.1.0
  * (REST API) The tap will now reduce the query window in half in the event of a timeout until it cannot reduce it further (down to a single second) [#28](https://github.com/singer-io/tap-zuora/pull/28)

## 1.0.7
  * Adds retry to the requests Session object, in the event that a long-lasting TCP connection gets snapped by Zuora [#27](https://github.com/singer-io/tap-zuora/pull/27)

## 1.0.6
  * Fixes some bugs discovered where v1/export queries would fail selected certain fields [#26](https://github.com/singer-io/tap-zuora/pull/26)

## 1.0.5
  * Fixes a bug where a bad record with a null bookmark can wipe out the state [#25](https://github.com/singer-io/tap-zuora/pull/25)

## 1.0.4
  * (AQuA API) The tap will now retry immediately in the event of an export job's timeout
  * State now tolerates a bookmark that is null, and will fall back to `start_date` in this case

## 1.0.3
  * (AQuA API) Replace the job_id retry pattern with a query window reduction pattern when a single job takes longer than the timeout.
  * Upgrade singer-python to 5.1.1

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
