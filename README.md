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

- [x] Переделать интерфейс PodSchedulingAlgorithm чтобы не возвращал ссылки с лайфтаймами (`return Result<String, SchedulerError>`)
- [x] Добавить коллбэки для остановки симулятора (SimulationCallback traits): step for duration + check lambda to stop simulation
- [x] Убрать опцию node_pool_capacity (она позже будет заменена опцией max_autoscaler_cluster_size). А вместо этой опции вычислять размер пула из трейса следующим образом: проходится в цикле по событиям, если событие - создание ноды, то +1 к счетчику, если событие - удаление/падение ноды, то обновление максимума нод, затем вычитание -1 из счетчика. Размер нод пула будет равным этому максимуму + некоторая дельта.

- [x] Печатать в simulation callbacks статистику или прогресс каждые n шагов, состояние кластера и тд
- [x] Переименовать get_max_simultaneously_existing_nodes_in_trace в понятное название, дать комментарий, что она делает
- [x] Сделать MetricsCollector
- [x] Отсортировать события в симуляторе в функции convert_to_simulator_events
- [x] Симулировать время работы планирования пода в зависимости от размера нод, которые мы перебираем. Время планирования на одну ноду пока считать константнтым.
- [x] Принимать &HashMap вместо Vec<&Node> в schedule one функции интерфейса

- [x] Сделать машиночитаемый формат сериализации метрик (json, etc)
- [x] Сделать отдельную очередь для бэкоффов для пода
- [x] Переименовать processed_pods в pods_terminated
- [x] При обновлении condition у pod/node выставлять last_transition_time равный времени происшествия события на непосредственной компоненте.
- [x] По метрикам: добавить pods_failed, тогда total_pods_in_trace = succeeded + pods unschedulable + pods_failed, переименовать pod_schedule_time_stats в scheduling_algorithm_latency_stats, добавить метрику pod_scheduling_duration_stats = pod_queue_time + scheduling_algorithm_latency
- [x] Вычислять размер пула с учетом размеров нод груп в автоскейлере

- [x] Улучшить интерфейс cluster autoscaler-a: + сделать из ClusterAutoscalerResponse enum?
```
// interface: trait ScaleUpAlgorithm 
// fn autoscale(&mut self, info: Info) -> Vec<Action> Action=Create/Remove node request
// leave default behaviour as k8s does
// flag for cluster autoscaler request to request only needed information
```
- [x] Подумать над тем, как сделать приоритеты в перемещении подов из unschedulable queue в hashmap (BTreeMap?)
- [x] Убрать недетерминированность с помощью замены HashMap на BTreeMap
- [x] Добавить в ClusterAutoscalerAlgorithm ссылку на стейт в autoscale, а сам стейт хранить в ClusterAutoscaler
```
// fn autoscale(&mut self, info: AutoscaleInfo, node_groups: &mut NodeGroups) -> Vec<AutoscaleAction>;
```
- [x] Сделать в ClusterAutoscaler-e общий max_node_count, а node count по каждой группе оставить
Option
- [x] Для long running сервисов сделать Option<running_duration>, где None - long running
- [x] Написать SimulationCallback для Long running service + Batch tasks
- [ ] Взять точку отсчета времени для resource usage для каждого пода от начала создания его Pod Group.
- [ ] Вынести цикл итерации по под группам в horizontal_pod_autoscaler

- [ ] Поэксперементировать с конвертацией alibaba trace в generic для ускорения загрузки и парсинга трейса с диска
- [ ] Подумать над тем, как реализовать schedule_one (под опцией в конфиге), вызываемый на каждое событие от persistent_storage - PodFinishedRunning/AddNodeToCacheRequest/PodScheduleRequest
- [ ] Добавить контекст в сам симулятор для того, чтобы логировать время через log_info!
- [ ] Моделирование отказов?
- [ ] ? Сделать ограничение количества подов на ноде, которое по умолчанию - не ограничено
- [ ] Убрать обертку для EstimatorWrapper
- [ ] Вычислять метрики: время работы ноды x кол-во cpu - процессорное время + получаем суммарное время со всех нод. Также для памяти - cpu seconds/memory seconds
- [ ] В результате препроцессинга трейсов выяснить, поместятся ли все поды на ноды из трейсов
- [ ] !Лучше реализовать Push модель сбора метрик по подам с каждой ноды, чтобы имитировать задержку отправки метрик более явно, нежели чем через задержку запроса в api server в pull модели.
- [ ] Реализовать трейт для резолвера имплементаций планировщика, автоскейлеров, чтобы пользователи могли предоставлять свой резолвинг без изменения кода в самой библиотеке.
