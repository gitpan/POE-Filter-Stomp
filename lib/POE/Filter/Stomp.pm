#
# File: Stomp.pm
# Date: 30-Aug-2007
# By  : Kevin Esteb
#
# This module will parse the input stream and create Net::Stomp::Frame 
# objects from that input stream. A STOMP frame looks like this:
#
#    command<lf>
#    headers<lf>
#    <lf>
#    body
#    \000
#
# More information is located at http://stomp.codehaus.org/Protocol
#

package POE::Filter::Stomp;

use 5.008;
use strict;
use warnings;

use Net::Stomp::Frame;
use IO::String;

use constant EOL => "\x0A";
use constant EOF => "\000";

our $VERSION = '0.01';

# ---------------------------------------------------------------------
# Public methods
# ---------------------------------------------------------------------

sub new {
	my $proto = shift;

	my $self = {};
	my $class = ref($proto) || $proto;

	$self->{buffer} = [];

	bless($self, $class);

	return $self;

}

sub get_one_start {
	my ($self, $buffers) = @_;
	
	$buffers = [$buffers] unless (ref($buffers));
	push (@{$self->{buffer}}, @{$buffers});
	
}

sub get_one {
	my ($self) = shift;

	my $frame;
	my $buffer;
	my $ret = [];

	if ($buffer = shift(@{$self->{buffer}})) {

		$frame = $self->_parse_frame($buffer);
		push(@$ret, $frame);

	}

	return $ret;

}

sub get {
	my ($self, $buffers) = @_;

	my $frame;
	my $ret = [];

	foreach my $buffer (@$buffers) {

		$frame = $self->_parse_frame($buffer);
		push (@$ret, $frame);

	}

	return $ret;

}

sub put {
	my ($self, $frames) = @_;

	my $string;
	my $ret = [];

	foreach my $frame (@$frames) {

		# protocol spec is unclear about the case of the command,
		# so uppercase the command, Why, just because I can.
		
		my $command = uc($frame->command);
		my $headers = $frame->headers;
		my $body = $frame->body;

		$string = $command . EOL;

		if ($headers->{bytes_message}) {

			delete $headers->{bytes_message};
			$headers->{'content-length'} = length($body);

		}

		# protocol spec is unclear about spaces between headers and values
		# nor the case of the header, so add a space and lowercase the 
		# header. Why, just because I can.

		while (my ($key, $value) = each %{$headers || {} }) {

			$string .= lc($key) . ': ' . $value . EOL;

		}

		$string .= EOL;
		$string .= $body || '';
		$string .= EOF;

		push (@$ret, $string);

	}

	return $ret;

}

# ---------------------------------------------------------------------
# Private methods
# ---------------------------------------------------------------------

sub _parse_frame {
	my ($self, $buffer) = @_;

	my $io = IO::String->new($buffer);
	my $byte;
	my $body;
	my $holder;
	my $headers;
	my $command;

	# read the command

	$command = $io->getline;
	chop $command;

	# read the headers

	while (1) {

		$holder = $io->getline;
		chop $holder;
		last if ($holder eq "");

		my ($key, $value) = split(': ?', $holder, 2);
		$headers->{$key} = $value;

	}

	# read the body
	#
	# if "content-length" is defined then the body is binary, so
    # create a "bytes_message" header to go along with the binary body.
	# "bytes_message" is used internally and is not part of the protocol.

	if ($headers->{'content-length'}) {

		$io->read($body, $headers->{'content-length'});
		$io->getc; # consume the EOF
		$headers->{bytes_message} = 1;

	} else {

		# OK, no "content-length", so consume the buffer until EOF
		# is found or end of buffer, whichever is first (malformed frame???).

		my $length = length($buffer) - $io->tell;

		for (my $x = 0; $x < $length; $x++) {

			$byte = $io->getc;
			last if ($byte eq EOF);
			$body .= $byte;

		}

	}

	# create the frame

	return (Net::Stomp::Frame->new({command => $command,
								   headers => $headers,
                                   body => $body}));

}

1;

__END__
# Below is stub documentation for your module. You'd better edit it!

=head1 NAME

POE::Filter::Stomp - Perl extension for the POE Environment

=head1 SYNOPSIS

  use POE::Filter::Stomp;

  For a server

  POE::Component::Server::TCP->new(
      ...
      Filter => 'POE::Filter::Stomp',
      ...
  );

  For a client

  POE::Component::Client::TCP->new(
      ...
      Filter => 'POE::Filter::Stomp',
      ...
  );

=head1 DESCRIPTION

This module is a filter for the POE environment. It will translate the input
buffer into Net::Stomp::Frame objects and serialize the output buffer from 
said objects. For more information an the STOMP protocol, please refer to: 
http://stomp.codehaus.org/Protocol .

This module supports both the get_one_start()/get_one() usage, along with 
the older get() usage.

=head1 EXPORT

None by default.

=head1 SEE ALSO

See the documentation for POE::Filter for usage.

=head1 BUGS

Quite possibly. It works for me, maybe it will work for you.

=head1 AUTHOR

Kevin L. Esteb, E<lt>kesteb@wsipc.orgE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2007 by Kevin L. Esteb

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.8.8 or,
at your option, any later version of Perl 5 you may have available.

=cut
