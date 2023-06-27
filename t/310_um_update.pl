# This test case runs the command:
# ./nc um update

use strict;
use warnings;

use File::Which;
#use PostgreSQL::Test::Cluster;
#use PostgreSQL::Test::Utils;
use Test::More tests => 1;
use IPC::Cmd qw(run);
use Try::Tiny;
use JSON;

#
# First, we find the list of available components with ./nc um update
# 

my $cmd = qq(./nc um update);
diag("cmd = $cmd\n");
my ($success, $error_message, $full_buf, $stdout_buf, $stderr_buf)= IPC::Cmd::run(command => $cmd, verbose => 0);

#
# Success is a boolean value; 0 means false, any other value is true. 
#
diag("success = $success");
diag("error_message = $error_message");
diag("stdout_buf = @$stdout_buf\n");

my $version = $success;

if (defined($version))
{
    ok(1);
}
else
{
    ok(0);
}

done_testing();
