CONTAINER_DIR=containers/
run:
	make run-zookeeper && make run-broker
remove:
	make remove-broker && make remove-zookeeper
run-zookeeper:
	docker-compose -f $(CONTAINER_DIR)/zookeeper.yml up -d
run-broker:
	docker-compose -f $(CONTAINER_DIR)/broker.yml up -d
remove-broker:
	docker-compose -f $(CONTAINER_DIR)/broker.yml stop
remove-zookeeper:
	docker-compose -f $(CONTAINER_DIR)/zookeeper.yml stop
