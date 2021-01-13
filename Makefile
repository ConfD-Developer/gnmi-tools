######################################################################
# Interface Status example
# (C) 2006-2009 Tail-f Systems
#
# See the README file for more information
######################################################################

usage:
	@echo "See README file for more instructions"
	@echo "make all     Build all example files"
	@echo "make clean   Remove all built and intermediary files"
	@echo "make start   Start CONFD daemon and example agent"
	@echo "make stop    Stop any CONFD daemon and example agent"
	@echo "make cli     Start the CONFD Command Line Interface"

######################################################################
# Where is ConfD installed? Make sure CONFD_DIR points it out
CONFD_DIR ?= ../../..

# Include standard ConfD build definitions and rules
include $(CONFD_DIR)/src/confd/build/include.mk

# In case CONFD_DIR is not set (correctly), this rule will trigger
$(CONFD_DIR)/src/confd/build/include.mk:
	@echo 'Where is ConfD installed? Set $$CONFD_DIR to point it out!'
	@echo ''


######################################################################
# Example specific definitions and rules

CONFD_FLAGS ?= --addloadpath $(CONFD_DIR)/etc/confd
#EXTRA_LINK_FLAGS +=-F rt:router-id
START_FLAGS ?=
SRC_DIR=src
PCG_DIR=$(SRC_DIR)


#all:	ietf-ip.fxs ietf-interfaces.fxs ietf-ipv6-unicast-routing.fxs \
#        ietf-ipv4-unicast-routing.fxs ietf-routing.fxs $(CDB_DIR) ssh-keydir
#	@echo "Build complete"

all: gnmi_proto \
	iana-if-type.fxs ietf-interfaces.fxs $(PCG_DIR)/ietf_interfaces_ns.py \
	$(PCG_DIR)/ietf_netconf_monitoring_ns.py \
	$(CDB_DIR) ssh-keydir init_interfaces.xml
	@echo "Build complete"

$(PCG_DIR)/ietf_netconf_monitoring_ns.py: $(CONFD_DIR)/etc/confd/ietf-netconf-monitoring.fxs
	$(CONFDC) --emit-python $@ $<

$(PCG_DIR)/ietf_interfaces_ns.py: ietf-interfaces.fxs
	$(CONFDC) --emit-python $@ $<

init_interfaces.xml:
	  ./datagen.py 100

gnmi_proto:
	python -m grpc_tools.protoc -I$(PCG_DIR)/proto --python_out=$(PCG_DIR) --grpc_python_out=$(PCG_DIR) $(PCG_DIR)/proto/gnmi.proto $(PCG_DIR)/proto/gnmi_ext.proto

test:
	PYTHONPATH=src pytest -sv

######################################################################
clean:	iclean
	rm -rf $(PCG_DIR)/gnmi_pb2.py $(PCG_DIR)/gnmi_pb2_grpc.py \
           $(PCG_DIR)/gnmi_ext_pb2.py $(PCG_DIR)/gnmi_ext_pb2_grpc.py \
           $(PCG_DIR)/*__.py $(PCG_DIR)/*_ns.py \
           .pytest_cache init_interfaces.xml


######################################################################

start:  stop
	### Start the confd daemon with our example specific confd-config
	$(CONFD) -c confd.conf $(CONFD_FLAGS)
	netconf-console --edit-config=init_interfaces.xml

######################################################################
stop:
	### Killing any confd daemon
	$(CONFD) --stop    || true

######################################################################
cli: cli-c

######################################################################
cli-c:
	$(CONFD_DIR)/bin/confd_cli -C --user=admin --groups=admin \
		--interactive || echo Exit

