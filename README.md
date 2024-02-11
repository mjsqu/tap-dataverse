# tap-dataverse

`tap-dataverse` is a Singer tap for Dataverse.

Built with the [Meltano Tap SDK](https://sdk.meltano.com) for Singer Taps.

## Installation

Install from GitHub:

```bash
pipx install git+https://github.com/mjsqu/tap-dataverse.git@main
```

## Configuration

## Capabilities

* `catalog`
* `state`
* `discover`
* `about`
* `stream-maps`
* `schema-flattening`
* `batch`

## Settings

| Setting             | Required | Default | Description |
|:--------------------|:--------:|:-------:|:------------|
| client_secret       | True     | None    | The client secret to authenticate against the API service |
| client_id           | True     | None    | Client (application) ID |
| tenant_id           | True     | None    | Tenant ID   |
| start_date          | False    | None    | The earliest record date to sync NOT WORKING see [#4](https://github.com/mjsqu/tap-dataverse/issues/4) |
| api_url             | True     | None    | The url for the API service |
| api_version         | False    | 9.2     | The API version found in the /api/data/v{x.y} of URLs |
| annotations         | False    | False   | Turns on annotations |
| sql_attribute_names | False    | False   | Uses the Snowflake column name rules to translate any characters outside the standard to an underscore. Particularly helpful when annotations are turned on |
| streams             | False    | None    | An array of streams, designed for separate paths using thesame base url. |
| stream_maps         | False    | None    | Config object for stream maps capability. For more information check out [Stream Maps](https://sdk.meltano.com/en/latest/stream_maps.html). |   
| stream_map_config   | False    | None    | User-defined config values to be used within map expressions. |
| faker_config        | False    | None    | Config for the [`Faker`](https://faker.readthedocs.io/en/master/) instance variable `fake` used within map expressions. Only applicable if the plugin specifies `faker` as an addtional dependency (through the `singer-sdk` `faker` extra or directly). |
| flattening_enabled  | False    | None    | 'True' to enable schema flattening and automatically expand nested properties. |
| flattening_max_depth| False    | None    | The max depth to flatten schemas. |
| batch_config        | False    | None    |             |

A full list of supported settings and capabilities for this
tap is available by running:

```bash
tap-dataverse --about
```

### Configure using environment variables

This Singer tap will automatically import any environment variables within the working directory's
`.env` if the `--config=ENV` is provided, such that config values will be considered if a matching
environment variable is set either in the terminal context or in the `.env` file.

### Source Authentication and Authorization

This tap uses the client_credentials method of authentication and requires an App Registration and PowerApp setup steps. Note, these steps have been copied from internal documentation with all identifying values removed, please refer to the official Dataverse API documentation for further assistance, or do a PR if you would like to help improve the docs.

#### Azure App Registration

- Login to https://portal.azure.com using your Azure Admin account
- Open App registrations
- Click + New registration
- Enter a Name (e.g. `tap_dataverse_powerplatformaccess`)
- Select Accounts in this organizational directory only (single tenant)
- Click Register
- Click Certificates & secrets
- Click + New client secret
- Description - (suggestion Tap-Dataverse Power Platform Client Secret)
- Select Expiry of choice
- Click Add and record `client_secret` in config
- Click Expose an API
- Click Set
- Update Application ID URI
- Enter the OAuth Endpoint
- Click + Add a scope
- Enter:
- Scope name = session:role-any
- Who can consent? = Admin and users
- Click Add scope
- Click Save

#### Configure the PowerApp
- Login to https://admin.powerplatform.microsoft.com
- Click Environments
- Click Data Provider environment
- Click S2S apps See all
- Click + New app user
- Click + Add an app
- Select `tap_dataverse_powerplatformaccess` or the name selected for the App Registration earlier
- Click Add
- Enter: Business unit from the PowerPlatform developer settings url e.g. from https://<my-business-unit>.api.crm6.dynamics.com the business unit value comes before .api.crm in the url

- Once you have completed all these steps, you should have:
- `client_id` - GUID
- `tenant_id` - GUID <> client_id
- `client_secret`

## Usage

You can easily run `tap-dataverse` by itself or in a pipeline using [Meltano](https://meltano.com/).

### Executing the Tap Directly

```bash
tap-dataverse --version
tap-dataverse --help
tap-dataverse --config CONFIG --discover > ./catalog.json
```

## Developer Resources

Follow these instructions to contribute to this project.

### Initialize your Development Environment

```bash
pipx install poetry
poetry install
```

### Create and Run Tests

Create tests within the `tests` subfolder and
  then run:

```bash
poetry run pytest
```

You can also test the `tap-dataverse` CLI interface directly using `poetry run`:

```bash
poetry run tap-dataverse --help
```

### Testing with [Meltano](https://www.meltano.com)

_**Note:** This tap will work in any Singer environment and does not require Meltano.
Examples here are for convenience and to streamline end-to-end orchestration scenarios._

<!--
Developer TODO:
Your project comes with a custom `meltano.yml` project file already created. Open the `meltano.yml` and follow any "TODO" items listed in
the file.
-->

Next, install Meltano (if you haven't already) and any needed plugins:

```bash
# Install meltano
pipx install meltano
# Initialize meltano within this directory
cd tap-dataverse
meltano install
```

Now you can test and orchestrate using Meltano:

```bash
# Test invocation:
meltano invoke tap-dataverse --version
# OR run a test `elt` pipeline:
meltano elt tap-dataverse target-jsonl
```

### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to use the SDK to
develop your own taps and targets.
