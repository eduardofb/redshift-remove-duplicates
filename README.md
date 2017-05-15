# Redshift Remove Duplicates

Simple script to remove duplicates entries from a Redshift cluster. Redshift doesn't enforce unique keys for id, so your application (eg.: ETL) needs to remove duplicates entries for the cluster.

If the table contains PK/FK constraints defined, this script will not work, because using this Redshift remove the command for UNIQUE returns. For more see the references. 

# Usage
`python redshift-remove-duplicates --db=redshift db-user=redshift db-pwd=redshift db-host=redshift-us-east1.amazonaws.com db-port=5439 schema-name=public`

# TODO
* Run ANALYZE and VACUUM;
* Add search path if the schema tables are not visible;
* Add default value (public) to schema name;
* Add parameter to include tables;
* Move to S3 and then use COPY command.

# Known issues
* For large tables maybe you need to increase nodes number;

## References
http://docs.aws.amazon.com/redshift/latest/dg/t_Defining_constraints.html