cmake_minimum_required(VERSION 2.6)
project (Grid)

macro(test_CLI var msg)
	if (${var})
		MESSAGE(STATUS "FOUND CLI ${msg} : ${${var}}")
	else (${var})
		MESSAGE(FATAL_ERROR "NOT FOUND CLI ${msg}")
	endif (${var})
endmacro(test_CLI)

macro(test_FOUND var msg)
	if (${var})
		MESSAGE(STATUS "FOUND ${msg} : ${${var}}")
	else (${var})
		MESSAGE(FATAL_ERROR "NOT FOUND ${msg}")
	endif (${var})
	unset(${var} CACHE)
endmacro(test_FOUND)

include(CheckIncludeFile)
include(CheckLibraryExists)
include(CheckTypeSize)

if (NOT ABI_VERSION)
	set(ABI_VERSION 0)
endif()

set(CMAKE_C_FLAGS "-g -fPIC -pipe -Wall -Wextra -std=gnu99")

if (CMAKE_COMPILER_IS_GNUCC)
    EXEC_PROGRAM(gcc ARGS --version OUTPUT_VARIABLE CMAKE_C_COMPILER_VERSION)

	# Activates the stack protection code when explicitely triggered
	# and with a decently recent gcc (>= 4.2)
    if(CMAKE_C_COMPILER_VERSION MATCHES " [4-9]\\.[2-9]\\.[0-9]+")
		if (STACK_PROTECTOR)
			set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fstack-protector-all")
		endif()
	endif()

	# Activates extra warnings that are available in recents gcc.
	# Some appeared well-managed since 4.4, others since 4.6.
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wunused")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wno-variadic-macros")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wsequence-point")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wredundant-decls")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wshadow")

    if(CMAKE_C_COMPILER_VERSION MATCHES " [4-9]\\.[4-9]\\.[0-9]+")
        set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wcomment")
        set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wmain")
        set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wparentheses")
        set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wfloat-equal")
        set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wunsafe-loop-optimizations")
        set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wunused-but-set-parameter")
        set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wunused-but-set-variable")
    endif()

	if(CMAKE_C_COMPILER_VERSION MATCHES " [4-9]\\.[6-9]\\.[0-9]+")
		set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wmissing-prototypes")
		#set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wconversion")
		#set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wswitch-enum")
	endif()
endif()

set(CMAKE_C_FLAGS_DEBUG          "-O0 -fno-inline")
set(CMAKE_C_FLAGS_RELEASE        "-O2")
set(CMAKE_C_FLAGS_RELWITHDEBINFO "-O2 -fno-inline")
set(CMAKE_C_FLAGS_MINSIZEREL     "-Os -s")
set(CMAKE_INSTALL_PREFIX "/usr/local")

add_definitions(-DAPI_VERSION="${API_VERSION}")
add_definitions(-DSHORT_API_VERSION="${SHORT_API_VERSION}")

if (CMAKE_BUILD_TYPE STREQUAL "Debug" OR CMAKE_BUILD_TYPE STREQUAL "RelWithDebInfo")
MESSAGE("DEBUG symbols enabled")
add_definitions(-DHAVE_EXTRA_ASSERT=1)
add_definitions(-DHAVE_ASSERT_EXTRA=1)
add_definitions(-DHAVE_EXTRA_DEBUG=1)
add_definitions(-DHAVE_DEBUG_EXTRA=1)
endif()

if (DEFINED SOCKET_OPTIMIZED)
	MESSAGE("SOCKET_OPTIMIZED ${SOCKET_OPTIMIZED}")
	add_definitions(-D_GNU_SOURCE)
	add_definitions(-DHAVE_SOCKET3)
	add_definitions(-DHAVE_ACCEPT4)
endif()

if (DEFINED SOCKET_LINGER_ONOFF)
	MESSAGE("SOCKET_LINGER_ONOFF ${SOCKET_LINGER_ONOFF}")
	add_definitions(-DSOCK_DEFAULT_LINGER_ONOFF=${SOCKET_LINGER_ONOFF})
endif ()

if (DEFINED SOCKET_LINGER_DELAY)
	MESSAGE("SOCKET_LINGER_DELAY ${SOCKET_LINGER_DELAY}")
	add_definitions(-DSOCK_DEFAULT_LINGER_DELAY=${SOCKET_LINGER_DELAY})
endif ()

add_definitions(-D_REENTRANT)
add_definitions(-D_LARGE_FILES)
add_definitions(-D_LARGEFILE_SOURCE)
add_definitions(-D_LARGEFILE64_SOURCE)
add_definitions(-D_FILE_OFFSET_BITS=64)
add_definitions(-DG_ERRORCHECK_MUTEXES)
add_definitions(-DHAVE_SOCKLEN_T)

if (DEFINED ALLOW_DEPRECATED)
	if (ALLOW_DEPRECATED)
		#add_definitions(-DG_DISABLE_DEPRECATED=1)
		set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wno-deprecated-declarations")
	endif ()
endif ()


###-------------------------------------------------------------------------###

if (LD_LIBDIR)
	MESSAGE("LD_LIBDIR explicitely set to ${LD_LIBDIR}")
else()
	CHECK_TYPE_SIZE(long SIZEOF_LONG)
	MESSAGE(STATUS "sizeof(long) = ${SIZEOF_LONG}")
	if(SIZEOF_LONG EQUAL 8)
		set (LD_LIBDIR "lib64")
	else(SIZEOF_LONG EQUAL 8)
		set (LD_LIBDIR "lib")
	endif(SIZEOF_LONG EQUAL 8)
endif()

if (PREFIX)
	set(CMAKE_INSTALL_PREFIX ${PREFIX})
endif (PREFIX)
MESSAGE(STATUS "Installation prefix : ${CMAKE_INSTALL_PREFIX}")

###-------------------------------------------------------------------------###

find_package(PkgConfig)
if (PkgConfig_FOUND)
else (PkgConfig_FOUND)
endif (PkgConfig_FOUND)

pkg_check_modules(GLIB2 REQUIRED glib-2.0 gthread-2.0 gmodule-2.0)
pkg_check_modules(JSONC json)
pkg_check_modules(JSONC json-c)

###--------------------------###
### Dependency to RedCurrant ###
###--------------------------###
set(REDCURRANT_INCLUDE_DIRS "/usr/include")
set(REDCURRANT_LIBRARY_DIRS "/${LD_LIBDIR}")
if (REDCURRANT_LIBDIR)
	set(REDCURRANT_LIBRARY_DIRS "${REDCURRANT_LIBDIR}")
endif(REDCURRANT_LIBDIR)
if (REDCURRANT_INCDIR)
	set(REDCURRANT_INCLUDE_DIRS ${REDCURRANT_INCDIR})
endif (REDCURRANT_INCDIR)

###-------------------------------------------------------------------------###

set(CMAKE_LIBRARY_PATH "")
set(CMAKE_INCLUDE_PATH "")

include_directories(BEFORE
		.)

include_directories(AFTER
		${GLIB2_INCLUDE_DIRS}
		${REDCURRANT_INCLUDE_DIRS}
		${JSONC_INCLUDE_DIRS})

link_directories(
		${GLIB2_LIBRARY_DIRS}
		${REDCURRANT_LIBRARY_DIRS}
		${JSONC_LIBRARY_DIRS})

add_executable(metacd_http server/metacd_http.c)

target_link_libraries(metacd_http
		metautils metacomm server hcresolve
		gridcluster gridcluster-remote
		meta2v2remote meta2v2utils meta2servicesremote
		meta1remote
		${GLIB2_LIBRARIES} ${JSONC_LIBRARIES})

install(TARGETS metacd_http 
		LIBRARY DESTINATION ${LD_LIBDIR}
		RUNTIME DESTINATION bin)

