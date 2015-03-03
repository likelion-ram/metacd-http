# metacd-http

RedCurrant HTTP proxy for META{0,1,2} services

For protocolar information, please visit the PROTOCOL.md 

## Install

The makefile generation is assumed by ``cmake``.

metacd-http depends on these tools and libraries:
  * [http://github.com/redcurrant/redcurrant][RedCurrant] currently not detected by pkg-config, you have to manually provide  the Redcurrant's installation paths if it doesn't lies in standard places.
  * [json-c] : detected with pkg-config, ovveriden if options manually specified.
  * [GLib-2.0] : detected with pkg-config, not overriden by options.

These options are managed:
  * REDCURRANT_INCDIR
  * REDCURRANT_LIBDIR
  * JSONC_INCDIR
  * JSONC_LIBDIR

