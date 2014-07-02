# metacd-http

RedCurrant HTTP proxy for META{0,1,2} services

## Conventions

We talk about *URL prefix* and *URL options*. This concern the path of the URL. The options are concatened using '/'.

In the following description, a few ``${...}`` elements are presented. For each element, here are the constraints on the element's values:
  * ``${NS}`` : a known namespace, typically a string with the form of a domain name. The physical namespace is the main token when read from left to right. According to Redcurrant's standards, a namespace token should not contain any character that is not alphanumerical.
  * ``${TYPE}`` :  a service type, single (e.g. ``meta2``) or compound (e.g. ``sqlx.plop``). the physical service type is the forst token when read from left to right. According to Redcurrant's standards, a service type's  token should not contain any character that is not alphanumerical.
  * ``${REF}`` : 
  * ``${PATH}`` : 
  * ``${POLICY}`` : a storage policy
  * ``${INT}`` : an integer in decimal form.

## Conscience operations

### Stateless load-balancing
  * URL Prefix: ``/lb/sl/`` 
  * URL Options:
    * ``ns/${NS}``
    * ``type/${TYPE}``

## Meta2 operations

### Content location resolution

  * URL Prefix: ``/m2/get/``
  * URL Options:
    * ``ns/${NS}``
    * ``ref/${REF}``
    * ``path/${PATH}``

### Content location provision

  * URL Prefix: ``/m2/beans/``
  * URL Options:
    * ``ns/${NS}``
    * ``ref/${REF}``
    * ``path/${PATH}``
    * ``size/${INT}``
    * ``policy/${POLICY}`` OPTIONAL

### Whole container listing

  * URL Prefix: ``/m2/list/``
  * URL Options:
    * ``ns/${NS}``
    * ``ref/${REF}``

## High-level Directory operations

### Container service listing

  * URL Prefix: ``/dir/list/``
  * URL Options:
    * ``ns/${NS}``
    * ``ref/${REF}``
    * ``type/${TYPE}``

### Simple stats about the directory cache usage

  * URL Prefix: ``/dir/status``

### Flushes the meta1's cache.
    
  * URL Prefix: ``/dir/flush/low``

### Flushes the conscience + meta0's cache.
    
  * URL Prefix: ``/dir/flush/high``

### Set various limits on internal caches

  * URL Prefix: ``/dir/set/ttl/low/``
  * URL Prefix: ``/dir/set/max/low/``
  * URL Prefix: ``/dir/set/ttl/high/``
  * URL Prefix: ``/dir/set/max/high/``
  * URL Options:
    * ${INT}
