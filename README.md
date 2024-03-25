# kubernetriks
kubernetriks = k8s + matrix ("What is the Matrix? Control." (Morpheus, The Matrix, 1999))

Framework to simulate k8s cluster, workload scheduling and autoscaling based on DSLab framework for distributed systems simulations.

Run prototype:
```
cargo test
RUST_LOG=trace cargo run -- --config-file src/config.yaml --trace-file src/data/generic_trace_example.yaml
```

### TODO
- [ ] Задавать кластер статически размером из конфига
```
# max_node_pool_size: K 
# cluster_size: N
# node_template1:
# template1_size: K 
# node_template2: 
(посмотреть dslab iaas)
```
- [ ] Создать отдельный файл событий изменения конфигурация кластера, не мешать его с подами
trace -> workload_trace.yaml/cluster_trace.yaml
- [ ] Модифицировать API Simulator-а для клинета для управления симуляцией.
```
// struct Simulator {
    // state
    // functions modifying state 
//}
```
- [ ] Переделать StorageData вместо отдельного объекта, поместить его внутрь только persistent storage
// storage_data: StorageData,
- [ ] У планировщика должна быть личная копия состояния которая должна обновляться асихронно на основе апдейтов
от persistent storage: добаление пода на ноду, снятие пода с ноды, падение/добавление нод в кластер
- [ ] Ограничить набор событий в SimulationEvent для трейса pub trait SimulatorEvent: EventData {} ??
- [ ] Можно упростить и убрать контейнеры в Pod 
- [ ] Доп фича - препроцессить трейс нод для того чтобы узнать количество для предварительной аллокации достаточного количества нод
