# kubernetriks
kubernetriks = k8s + matrix ("What is the Matrix? Control." (Morpheus, The Matrix, 1999))

Framework to simulate k8s cluster, workload scheduling and autoscaling based on DSLab framework for distributed systems simulations.

Run prototype:
```
cargo test
RUST_LOG=debug cargo run -- --config-file src/config.yaml
```

### TODO
- [x] Задавать кластер статически размером из конфига
```
# max_node_pool_size: K 
# cluster_size: N
# node_template1:
# template1_size: K 
# node_template2: 
(посмотреть dslab iaas)
```
- [x] Создать отдельный файл событий изменения конфигурация кластера, не мешать его с подами
trace -> workload_trace.yaml/cluster_trace.yaml
- [x] Модифицировать API Simulator-а для клиента для управления симуляцией.
```
// struct Simulator {
    // state
    // functions modifying state 
//}
```
- [x] Переделать StorageData вместо отдельного объекта, поместить его внутрь только persistent storage
// storage_data: StorageData,
- [x] У планировщика должна быть личная копия состояния которая должна обновляться асихронно на основе апдейтов
от persistent storage: добаление пода на ноду, снятие пода с ноды, падение/добавление нод в кластер
- [x] Ограничить набор событий в SimulationEvent для трейса pub trait SimulatorEvent: EventData {} ??
- [x] Можно упростить и убрать контейнеры в Pod 
- [x] Доп фича - препроцессить трейс нод для того чтобы узнать количество для предварительной аллокации достаточного количества нод

- [x] Вынести тело цикла в отдельную функцию + сделать более понятную нумерацию нод (1,2,3) - падать если имя неуникально + сделать структуру удобную как у https://github.com/osukhoroslov/dslab/blob/4ea3b3a4abe0b36dca5359a838d502dacf9da95d/crates/dslab-iaas/src/core/config/sim_config.rs#L42
- [x] NodeBundle -> NodeGroup
- [x] Поднять очередь подов для планировщика, в которую будут складываться поды по запросу от Persistent storage. Завести цикл планирования, который в своей итерации выгребает всю очередь подов, планирует каждый под, замеряет планирование каждого пода + полностью итерацию планирования. Назначение нового цикла планирования происходит с задержкой = max(scheduling period, current scheduling time). Отправка запроса AssignPodToNodeRequest происходит с задержкой = сетевой задержке + время проведенное подом в очереди + его время планирования.
- [x] Заменить debug!/info! на dslab::log_debug/log_info
- [x] Сделать два трейта для планировщика:
```
// trait AnyScheduler
// fn schedule_one(pod: &Pod, nodes: [Nodes]) -> Result<String, ScheduleError>;

// trait KubeGenericScheduler
// fn filter
// fn score
```
- [x] Перенести всю логику Cluster controller-a в node pool и убрать cluster controller, обойтись только api server + node pool
