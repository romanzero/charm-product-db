Charm Product DB
================

An API for interacting with charm product data stored in DynamoDB

Installation
-------------

Configure an SSH key for access to this github repository and install using `pip`:

```
pip install git+ssh://git@github.com/dwurtz/charm-product-db.git@vMAJOR.MINOR.PATCH
```

Configuration
-------------
In order to use the `charm_product` module, you must set the `CHARM_PRODUCT_ENV`
environment variable to one of `dev`, `staging`, or `prod`. This determines the
names of the DynamoDB tables that will be used by the API. DynamoDB table names
are automatically prefixed with `charm_${CHARM_PRODUCT_ENV}_`.

You must also configure AWS settings for `boto3` to access your credentials and region.

https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html#configuring-credentials


DynamoDB Tables
---------------

In order to create the DynamoDB tables used by the API, you must run the
migration scripts located in `charm_product/schema` in order (script names are
prefixed with `migrate-NUMBER`). Set the `CHARM_PRODUCT_ENV` environment
variable to the environment for which tables will be created/updated.

```
CHARM_PRODUCT_ENV=dev python charm_product/schema/migrate_0001_create_product_tables.py
CHARM_PRODUCT_ENV=dev python charm_product/schema/migrate_0002_replace_product_tag_image_index.py
...
```

There is currently no process to undo migrations. You may manually delete the
DynamoDB tables using the AWS web console. This will, of course, result in
dropping all data from the tables, so it is only advisable to do this for
`staging` or `dev` tables.

Development
-----------

### Testing

Test requirements can be isntalled with `pip`.

```
pip install -r requirements-dev.txt
```

Unit tests are located in the `tests` directory. There is a `Makefile` for
running tests. You may run all tests with `make coverage` and may check the
code for syntax errors and styling issues by running `make lint`.

### API Version

The version of this library is managed using the python `bumpversion` utility.
In order to update the version, run `bumpversion {MAJOR|MINOR|PATCH}`. This
will create a commit that bumps the package version number and a tag in the
format `v{MAJOR}.{MINOR}.{PATCH}`

You must manually push the tag to the remote repo using `git` for bumpversion
commits that are merged into the `master` branch.

Use [semantic versioning](https://semver.org/) conventions to determine whether
to increase the major, minor, or patch version.
