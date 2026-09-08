PROTO_DIR=docs/proto
GO_OUT_DIR=common/api/protogen
VENDOR_DIR=vendor
PROTO_FILES := $(shell find $(PROTO_DIR) -type f -name '*.proto')

generate_proto:
	@echo "Generating files:" $(PROTO_FILES)
	mkdir -p $(GO_OUT_DIR)
	protoc -I $(PROTO_DIR) \
		$(PROTO_FILES) \
		--go_out=$(GO_OUT_DIR) \
		--go_opt=paths=source_relative \
		--go-grpc_out=$(GO_OUT_DIR) \
		--go-grpc_opt=paths=source_relative

