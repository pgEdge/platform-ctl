## ACE (Anti-Chaos Engine) User Guide

ACE is a powerful tool designed to ensure and maintain consistency across nodes in a pgEdge cluster. It helps identify and resolve data inconsistencies, schema differences, and replication configuration mismatches across nodes in a cluster.

Key features include:
- Table-level data comparison and repair
- Replication set level verification
- Automated repair capabilities
- Schema comparison
- Spock configuration validation

### Installing ACE

ACE is installed with the pgEdge Platform installer. The following commands describe installing ACE on a management system that is not a member of a replication cluster:

1. Navigate to the directory where ACE will be installed.

2. Invoke the pgEdge installer in that location with the command: 

`python3 -c "$(curl -fsSL https://pgedge-download.s3.amazonaws.com/REPO/install.py)` 

3. Create a directory named `cluster` in the `pgedge` directory created by the pgEdge installer. 

4. [Create and update a cluster_name.json file](https://docs.pgedge.com/platform/installing_pgedge/manage_json), and place the file in `cluster/cluster_name/cluster_name.json` on the ACE host.  For example, if your cluster name is `us_eu_backend`, the cluster definition file for this should be placed in `/pgedge/cluster/us_eu_backend/us_eu_backend.json`.  The .json file must: 

    * Contain connection information for each node in the cluster in the node's stanza.
    * Identify the user that will be invoking ACE commands in the `db_user` property.  This user must also be the table owner.
    
After ensuring that the .json file describes your cluster connections and identifies the ACE user, you're ready to use [ACE commands](#ace-commands).

## ACE Functions

To review online help about the ACE commands and syntax available for your ACE version, use the command:

`./pgedge ace [command_name] --help`

For detailed information about usage, recommended use cases, and optional arguments, see [the pgEdge documentation](https://designing.pgedge-docs-sandbox.pages.dev/platform/ace/using_ace#ace-commands).


## API Reference

ACE provides a REST API for programmatic access. The API server runs on `localhost:5000` by default. An SSH tunnel is required to access the API from outside the host machine for security purposes.

For detailed information about API usage, recommended use cases, and optional arguments, see [the pgEdge documentation](https://designing.pgedge-docs-sandbox.pages.dev/platform/ace/ace_api).


## Scheduling ACE Operations (Beta)

ACE supports scheduling of automated table-diff and auto-repair operations through configuration settings in the `ace_config.py` file. This allows for regular consistency checks and remediations without manual intervention. 

The `ace_config.py` file is located in `${PGEDGE_HOME}/hub/scripts/ace_config.py`. Within the file, you define jobs and their schedules with key/value pairs in the followiwng sections:

* the `schedule_jobs` section of the file contains information about ACE diff jobs.
* the `schedule_config` section of the file contains information about the run frequency.
* the `auto_repair_config` section of the file contains information about scheduled repair jobs.

The ACE scheduler runs the jobs defined in the `ace_config.py` file automatically when ACE is started, or you can control the manually. Use the following commands to manually start and stop the scheduler:

To start the scheduler:

```bash
./pgedge start ace
```

To stop the scheduler:

```bash
./pgedge stop ace
```

For detailed information about using ACE scheduling functionality, [see the pgEdge documentation](https://designing.pgedge-docs-sandbox.pages.dev/platform/ace/schedule_ace).
