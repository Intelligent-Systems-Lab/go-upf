# go-upf Embedded EES 設計邏輯與 3GPP 規範對齊分析報告

本報告針對 `go-upf` 中 `feat/embedded-ees` 分支的 Event Exposure Service (EES) 實作進行深入剖析，探討其核心設計邏輯以及與 3GPP 相關標準的對齊程度。

## 1. 核心設計邏輯 (Design Logic)

在現有版本中，EES 被直接嵌入（Embedded）於 UPF 中，這種設計允許 UPF 繞過傳統的 SMF/NEF 路徑，直接將用戶面的數據使用量與事件暴露給本地的邊緣應用（如 AF 或本地 EES），從而大幅降低邊緣運算的通知延遲。

### 1.1 架構與模組劃分
- **Dispatcher (分發器, `pkg/app/dispatcher.go`)**:
  - UPF 內部的數據上報採用了「多播 (Multicast)」架構。當底層 Forwarder (如 GTP5G kernel) 觸發 Usage Report (USAR) 時，Dispatcher 會將該 `SessReport` 同時分發給 PFCP Handler (負責與 SMF 溝通) 以及 EES Handler。
  - 此設計確保了 EES 模組可以與標準 PFCP 流程平行運作，而不會干擾既有的核心網控制面互動。
- **Aggregator (聚合器, `internal/ees/aggregator.go`)**:
  - **資料來源過濾**: Aggregator 接收到 USAR 後，會特別篩選 **URR ID 2 (N3N6_MAQE, Measurement After QoS Enforcement)** 的數據。這是因為 URR 2 代表了整個 PDU Session 實際傳輸的流量，完美契合 `PER_SESSION` (每 PDU 階段) 的測量粒度。
  - **Push Model 與緩衝**: 採用純 Push 模型，核心態的主動上報數據會先被緩衝在記憶體中。
  - **週期性觸發與動態對齊**: 透過 `TickOnce` 方法定期檢查緩衝區。為了保證測量準確性，Aggregator 支援 `AdjustReportPeriod`，能夠將 EES 的回報週期動態對齊到 URR 的測量週期 (urrPeriod) 的整數倍。
  - **資料整併 (Consolidation)**: 對於同一 Session 產生的多筆 Incremental Reports (增量報告)，會將 Volume (位元組/封包) 加總，並擴展時間窗 (最早的 StartTime 到最晚的 EndTime)，同時計算出吞吐量 (Throughput Bps/Pps)。
- **API Server (`internal/ees/api.go`) & Store**:
  - 提供 RESTful API 介面供訂閱者建立/刪除訂閱。
  - 訂閱條件高度過濾：目前 MVP 階段限制只接受 `USER_DATA_USAGE_MEASURES` 事件，以及 `PER_SESSION` 粒度。
  - 支援全域 (`AnyUE=true`) 或針對特定 IP (`UeIPAddress`) 的訂閱。
- **Notifier (`internal/ees/notifier.go`)**:
  - 負責將整併後的測量結果封裝成 JSON 格式，透過 HTTP POST 推送給訂閱時指定的 `NotifURI`。
  - Payload 的生成嚴格遵循 3GPP 規範的結構。

### 1.2 狀態與報告模式
支援兩種觸發模式 (`EventReportingMode.Trigger`)：
1. **PERIODIC (週期性)**: 每隔指定的 `ReportPeriod` 時間，將期間內累計的流量/封包數據打包發送。
2. **ON_DEMAND (單次/即時)**: (對應規範的 `ONE_TIME`) 收到訂閱時，立即觸發一次 `TickOnce` 將當前緩衝的數據送出，送出後在系統內部會自動轉為 `PERIODIC` 模式以便後續追蹤，但在 MVP 中主要用於滿足即時查詢需求。

---

## 2. 與 3GPP 規範的對齊情況 (Alignment with 3GPP Specs)

`go-upf` 的 Embedded EES 在 API 介面和資料模型上，高度參考並對齊了 **3GPP TS 29.564 (User Plane Function Services)** 以及邊緣運算相關的架構精神。

### 2.1 API Endpoint 與路徑
- **對齊 TS 29.564 5.3 節**: 
  - 建立訂閱：`POST /nupf-ee/v1/ee-subscriptions`
  - 刪除訂閱：`DELETE /nupf-ee/v1/ee-subscriptions/{subscriptionId}`
  - API Root 和版本號 (`v1`) 均符合 Nupf_EventExposure 服務的定義。

### 2.2 事件與粒度 (Event & Granularity)
- **Event Type**: 完整支援 `USER_DATA_USAGE_MEASURES` (用戶數據使用量測量)。架構上也預留了 `USER_DATA_USAGE_TRENDS` (數據使用趨勢) 的擴充點。
- **Granularity (測量粒度)**: 目前實作支援 `PER_SESSION` (對應 TS 29.564 預設行為)。程式碼結構已建立 `PER_APPLICATION` (需配合 `AppIds`) 與 `PER_FLOW` (需配合 `TrafficFilters`) 的驗證邏輯，為未來的細粒度 QoS 監控打下基礎。

### 2.3 測量類型與 Payload 模型
內部型別 (`types.go`) 與 `notifier.go` 的 JSON 封裝完全映射 TS 29.564 定義的 Data Types：
- **MeasurementTypes**:
  - `VOLUME_MEASUREMENT`: 提供 `TotalVolume`, `UlVolume`, `DlVolume` 及對應的封包數量。
  - `THROUGHPUT_MEASUREMENT`: 提供字串格式的 `UlThroughput` / `DlThroughput` (例如 `"1024 bps"`) 及 Packet Throughput。
- **NotificationData Schema**:
  - Notifier 推送的 JSON 結構嚴格包含 `NotificationItems` 陣列。
  - 每個 Item 包含 `EventType`, `TimeStamp`, `UeIpv4Addr`，以及核心的 `UserDataUsageMeasurements` 陣列。

### 2.4 PFCP (TS 29.244) 共用設計
- **無縫繼承**: 傳統上 UPF 需要額外配置 Shadow URR 才能滿足獨立的測量需求，這會增加 Kernel 負擔。`go-upf` 的創新在於 **「重用 SMF 建立的 URR 2 (N3N6_MAQE)」**。
- **合規且高效**: 根據 TS 29.244，URR 負責收集 Usage Report。Embedded EES 直接在 UPF 使用者空間 (User Space) 攔截並轉發這份報告，既不違反 PFCP 協議狀態機，又達成了免修改 SMF 即可提供邊緣數據暴露的目標。

---

## 3. 總結與未來展望

`go-upf` 目前在 `feat/embedded-ees` 分支上的實作，成功地將 3GPP TS 29.564 的核心功能 (Nupf_EventExposure) 以非常輕量、不干擾主流程的方式嵌入到資料面節點中。

**當前 MVP 特色**：
1. **效能優勢**：重用 URR 2，無須為 EES 額外增加 Kernel 層級的封包過濾負擔。
2. **標準相容**：REST API 與 JSON Schema 高度相容 TS 29.564。
3. **時間同步機制**：`AdjustReportPeriod` 確保了 EES 暴露的資料週期能與底層 URR 上報週期完美對齊，避免了資料錯位。

**未來可擴展方向**：
1. **進階 Granularity 支援**：實作 `PER_APPLICATION` 與 `PER_FLOW`，需搭配 PDI (Packet Detection Information) 和 Application ID 的細粒度過濾。
2. **支援 `THRESHOLD` 觸發**：目前僅支援 `PERIODIC`，未來可加入當流量達到特定門檻時立即觸發的機制。
3. **支援多 UE 的 TargetScope**：擴充至依據 SUPI、DNN 或 S-NSSAI 進行群體訂閱過濾。
