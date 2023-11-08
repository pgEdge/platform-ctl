# This is part of a complex test case; after creating a two node cluster on the localhost, 
# the test case executes the commands in the Getting Started Guide at the pgEdge website.
#
# In this case, we'll register node 1 and create the repset on that node.
# After creating the repset, we'll query the spock.replication_set_table to see if the repset exists. 


use strict;
use warnings;
use File::Which;
use IPC::Cmd qw(run);
use Try::Tiny;
use JSON;
use lib './t/lib';
use contains;
use edge;
use DBI;
use List::MoreUtils qw(pairwise);
no warnings 'uninitialized';

# Our parameters are:

print("whoami = $ENV{EDGE_REPUSER}\n");


# We can retrieve the home directory from nodectl in json form... 

my $json = `$ENV{EDGE_N1}/pgedge/nc --json info`;
# print("my json = $json");
my $out = decode_json($json);

$ENV{EDGE_HOMEDIR1} = $out->[0]->{"home"};
print("The home directory is $ENV{EDGE_HOMEDIR1}\n"); 

# We can retrieve the port number from nodectl in json form...
my $json1 = `$ENV{EDGE_N1}/pgedge/nc --json info $ENV{EDGE_VERSION}`;
# print("my json = $json1");
my $out1 = decode_json($json1);
$ENV{EDGE_PORT1} = $out1->[0]->{"port"};
print("The port number is $ENV{EDGE_PORT1}\n");



my $cmd5 = qq($ENV{EDGE_HOMEDIR1}/nodectl spock repset-create --replicate_truncate=False $ENV{EDGE_REPSET} $ENV{EDGE_DB});
print("cmd5 = $cmd5\n");
my ($success5, $error_message5, $full_buf5, $stdout_buf5, $stderr_buf5)= IPC::Cmd::run(command => $cmd5, verbose => 0);
print("stdout_buf5 = @$stdout_buf5\n");
print("We just executed the command that creates the replication set (demo-repset)\n");
print("\n");


if(!(contains(@$stdout_buf5[0], "repset_create")))
{
    exit(1);
} 


print("="x100,"\n");


##Table validation

my $dbh = DBI->connect("dbi:Pg:dbname=$ENV{EDGE_DB};host=$ENV{EDGE_HOST};port= $ENV{EDGE_PORT1}",$ENV{EDGE_USERNAME},$ENV{EDGE_PASSWORD});



my $table_exists = $dbh->table_info(undef, 'public', $ENV{EDGE_TABLE}, 'TABLE')->fetch;

if ($table_exists) {
    print "Table '$ENV{EDGE_TABLE}' already exists in the database.\n";
    
    print("\n");
} 

else
{
# Creating public.$ENV{EDGE_TABLE} Table


 
    my $cmd6 = qq($ENV{EDGE_HOMEDIR1}/$ENV{EDGE_VERSION}/bin/psql -t -h $ENV{EDGE_HOST} -p $ENV{EDGE_PORT1} -d $ENV{EDGE_DB} -c "CREATE TABLE $ENV{EDGE_TABLE} (col1 INT PRIMARY KEY)");
    
    print("cmd6 = $cmd6\n");
    
    my($success6, $error_message6, $full_buf6, $stdout_buf6, $stderr_buf6)= IPC::Cmd::run(command => $cmd6, verbose => 0);
    print("stdout_buf6 = @$stdout_buf6\n");
    
   if(!(contains(@$stdout_buf6[0], "CREATE TABLE")))
{
    exit(1);
}
   
   print ("-"x100,"\n"); 
   
  
     # Inserting into public.$ENV{EDGE_TABLE} table

   my $cmd7 = qq($ENV{EDGE_HOMEDIR1}/$ENV{EDGE_VERSION}/bin/psql -t -h $ENV{EDGE_HOST} -p $ENV{EDGE_PORT1} -d $ENV{EDGE_DB} -c "INSERT INTO $ENV{EDGE_TABLE} select generate_series(1,10)");

   print("cmd7 = $cmd7\n");
   my($success7, $error_message7, $full_buf7, $stdout_buf7, $stderr_buf7)= IPC::Cmd::run(command => $cmd7, verbose => 0);
      print("stdout_buf7 = @$stdout_buf7\n");
   if(!(contains(@$stdout_buf7[0], "INSERT")))
{
    exit(1);
}
   }
   
   print("="x100,"\n");
   
    
  #checking repset
  
  my $cmd9 = qq($ENV{EDGE_HOMEDIR1}/$ENV{EDGE_VERSION}/bin/psql -t -h $ENV{EDGE_HOST} -p $ENV{EDGE_PORT1} -d $ENV{EDGE_DB} -c "SELECT * FROM spock.replication_set where set_name='$ENV{EDGE_REPSET}'");
   print("cmd9 = $cmd9\n");
   my($success9, $error_message9, $full_buf9, $stdout_buf9, $stderr_buf9)= IPC::Cmd::run(command => $cmd9, verbose => 0);
   print("stdout_buf9= @$stdout_buf9\n");
  print("="x100,"\n");
  

  #
  # Listing repset tables 
  #
    my $json3 = `$ENV{EDGE_N1}/pgedge/nc spock repset-list-tables $ENV{EDGE_SCHEMA} $ENV{EDGE_DB}`;
   print("my json3 = $json3");
   my $out3 = decode_json($json3);
   $ENV{EDGE_SETNAME} = $out3->[0]->{"set_name"};
   print("The set_name is = $ENV{EDGE_SETNAME}\n");
   print("="x100,"\n");
   
#Adding Table to the Repset 

if($ENV{EDEGE_SETNAME} eq ""){
  
  
       my $cmd8 = qq($ENV{EDGE_HOMEDIR1}/nodectl spock repset-add-table $ENV{EDGE_REPSET} $ENV{EDGE_SCHEMA}.$ENV{EDGE_TABLE} $ENV{EDGE_DB});
    
     print("cmd8 = $cmd8\n");
     my($success8, $error_message8, $full_buf8, $stdout_buf8, $stderr_buf8)= IPC::Cmd::run(command => $cmd8, verbose => 0);
     print("stdout_buf8 = @$stdout_buf8\n");
     
     if(!(contains(@$stdout_buf8[0], "Adding table")))
{
    exit(1);
}
      
} 


else {
   print ("Table $ENV{EDGE_TABLE} is already added to $ENV{EDGE_REPSET}\n");
    
   
}

print("="x100,"\n");

# Then, use the info to connect to psql and test for the existence of the replication set.

my $cmd10 = qq($ENV{EDGE_HOMEDIR1}/$ENV{EDGE_VERSION}/bin/psql -t -h $ENV{EDGE_HOST} -p $ENV{EDGE_PORT1} -d $ENV{EDGE_DB} -c "SELECT * FROM spock.replication_set");
print("cmd10 = $cmd10\n");
my($success10, $error_message10, $full_buf10, $stdout_buf10, $stderr_buf10)= IPC::Cmd::run(command => $cmd10, verbose => 0);
#print("stdout_buf10 = @$stdout_buf10\n");

# Test to confirm that cluster is set up.



if(contains(@$stdout_buf10[0], "demo-repset"))

{
    exit(0);
}
else
{
    exit(1);
}


