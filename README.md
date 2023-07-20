# dbt Custom materialization
This is a simplified version of the actual requirements to demonstrate how we can create a custom dbt materialization. We could have created a custom *snapshot strategy* to fulfill this requirement but a custom materialization offers much more flexibility that was needed for the actual use case.

## Problem
We are trying to build history tables (scd type 2) using the dbt `snapshot`. But the out of the box snapshot strategy does not work 100% for us. So there are 3 options/solutions we can use on of:

1. Build a `view` (which will lead to building another layer with templated codes) on top of the out-of-the box snapshot. Also it can be time/resource consuming every time we query these views and this consumptions will be significant since history tables tend to be very big most of the times.
1. Use a `post_hook` but again, almost another layer to maintain. We could use a macro also for the hook but the 3rd option is even tidier.
1. Take the existing snapshot logic, customize it as per our requirements and that will be our own materialization. With this, we can just build our models just like snapshots without doing anything extra.

So we choose the 3rd option.


## Requirements
Now let's understand we do we even need that. Following is the set of requirements:

- `dbt_valid_to` column should have value `9999-12-31 23:59:58.9999999` instead of `null`
- Our definition of delete is slightly different from [dbt delete](https://docs.getdbt.com/docs/build/snapshots#hard-deletes-opt-in). We need to updated the `dbt_valid_to` column to have the current timestamp but also we need to insert one extra record for which `dbt_valid_from` will be set to current timestamp and `dbt_valid_to` will be set to `9999-12-31 23:59:58.9999999`.