# Garmin 健康数据：官方规则与用户反馈交叉核对

更新时间：2026-07-25

## 范围与证据等级

本页补充 `garmin-health-data-integration.md` 中仅靠接口/schema 无法回答的问题，重点核对
Garmin 官方支持文档、Garmin 官方论坛、`python-garminconnect`/GarminDB 项目资料
和少量真实用户反馈。

- **官方事实**：Garmin 支持文档、开发者文档或当前开源客户端源码。
- **已确认产品故障**：Garmin 员工在官方论坛确认、跟踪或宣布修复的问题。
- **用户经验**：论坛用户的单例或小样本反馈，只能帮助形成排查假设，不能当成产品保证。
- **本账号证据**：2026-07-25 已完成的低频只读能力审计；本轮研究没有访问 Garmin
  或 Home Assistant，也没有发起任何账号请求。

## 结论摘要

1. **当前现役 Forerunner 255 不支持 Garmin 的夜间皮肤温度。** 已卖掉的 Venu 3
   支持该功能，所以账号中仍可能保留它过去产生的皮温历史。FR255 活动记录中的
   `temperature` 是手表内部/环境影响很大的温度，不能当作夜间皮温。
2. **当前只有 FR255 时，不再存在两只表的实时职责分配问题。** 它应自然成为
   Primary Wearable 和 Primary Training Device；仍应检查 Connect 中是否残留旧
   Venu 3 的设备优先级。Primary Wearable 决定睡眠、HRV、压力等全天健康数据的主
   来源，Primary Training Device 决定训练状态、训练负荷等训练指标。
3. **Health Snapshot 和 ECG 在产品/UI 中确实存在，但不能等同于已有稳定私有 API。**
   FR255 支持 Health Snapshot，但不支持 ECG。已卖掉的 Venu 3 在台湾支持 ECG；
   如果过去实际做过 ECG，Connect/完整账户导出仍可能保留历史记录。论坛用户还确认
   完整账户导出可出现 ECG 与皮温 FIT 文件，但这不证明 `python-garminconnect`
   有稳定读取方法。
4. **空数组或断点不是零值，也不能直接解释成设备不支持。** 佩戴、松紧、低电量、
   Battery Saver、Pulse Ox 模式、Primary Wearable、同步故障、时区、账号关联和
   Garmin 的旧数据 offload 都可能造成缺口。
5. **限流最危险的已知案例集中在登录流程。** `python-garminconnect` #337 的 429
   明确发生在 OAuth `preauthorized` 登录端点，不是普通健康数据端点。普通私有 API
   的精确限额没有官方承诺；应复用 token、避免重复 SSO 登录、低频增量读取，并对
   429 立即停止而不是自动重试。

## 1. FR255 与历史 Venu 3 皮肤温度

### 官方事实

Garmin 当前皮温兼容列表包含 **Venu 3/Venu 3S**，不包含 **Forerunner 255**。
由于当前现役设备只有 FR255，之后不会产生 Garmin Nightly Skin Temperature 数据。
已卖掉的 Venu 3 过去可能产生过该数据；当时该功能：

- 只在佩戴兼容手表睡眠时采集；
- 要求该表是 **Primary Wearable**；
- 至少睡三晚后才建立基线并显示；
- Connect 展示的是相对个人正常体温的**夜间温度变化/偏差**，不是医疗体温计式的
  绝对核心体温。

来源：[Garmin 皮肤温度 FAQ（繁中）](https://support.garmin.com/zh-TW/?faq=RDXq6E5Iaq9rD1l1kTObf6)

FR255 的活动详情可能出现 `temperature` 图表，但官方论坛讨论将其解释为手表内部
温度；佩戴时会被皮肤、环境和设备自身热量共同影响，连接 Tempe 传感器时才是 Tempe
读数。这不是 Venu 3 的夜间 Skin Temperature 功能。

来源：[FR255 活动温度讨论](https://forums.garmin.com/sports-fitness/running-multisport/f/forerunner-255-series/300085/what-does-the-temperature-on-a-running-activity-refer-to-it-isn-t-the-local-weather-is-it-my-skin-temperature)

### 用户经验

- Venu 3 用户报告看不到皮温时，切换到睡眠的 7 日视图后才找到 Skin Temp Change。
  [Venu 3 论坛反馈](https://forums.garmin.com/sports-fitness/healthandwellness/f/venu-3-series/364409/skin-temprature-not-showing-in-venu-3)
- 换一只新设备并把它设为 Primary Wearable 后，有用户发现仍需重新等待三晚；旧设备
  已有多年基线也不会让新设备立即出值。这与官方“三晚建基线”规则一致。
  [换设备后的三晚校准反馈](https://forums.garmin.com/sports-fitness/healthandwellness/f/venu-x1/437838/skin-temperature-not-recorded)
- 其他型号曾发生固件导致 Connect 不显示已记录皮温的故障；Garmin 员工确认修复后，
  用户看到历史皮温回填。这说明“当日接口为空”有时是展示/同步问题，而不一定是传感器
  未记录。[Garmin 已确认并修复的皮温显示问题](https://forums.garmin.com/outdoor-recreation/outdoor-recreation/f/epix-2/353672/v15-74---skin-temperature-while-sleeping-missing-in-gc-for-the-tactix-7-amoled/1705715)

### 对本账号的解释

本账号审计在三个代表日都看到 `skinTempDataExists=false`。结合设备更正，最直接的
解释是这些夜晚由 FR255 记录，或当时 Venu 3 没有满足皮温采集条件。若只为盘点历史，
优先核对：

1. Venu 3 的实际持有和佩戴日期范围；
2. 对应夜晚是否佩戴 Venu 3，而不是 FR255；
3. 当时 Venu 3 是否为 Primary Wearable，并已完成至少三晚基线；
4. 官方完整账户导出中是否存在 `*_SKIN_TEMP.fit`。

三个 `false` 不能否定 Venu 3 持有期间可能存在的历史数据；但对未来而言，除非重新
购买兼容设备，否则 FR255 不会新增夜间皮温。

## 2. 多设备、Primary Wearable、PTD 与 TrueUp

### 官方事实

Garmin 把多设备优先级拆成两类：

| 设置 | 决定的主数据 |
|---|---|
| Primary Wearable | HRV Status、睡眠、压力及其他全天健康数据 |
| Primary Training Device (PTD) | Training Status、Training Effect、Training Load 等训练数据 |

Garmin 建议在可能时让同一设备承担两种角色；非 PTD 记录活动后，需要设备与 Connect
经过多次同步，PTD 才会重新计算并回传统一训练状态。

来源：[Garmin Unified Training Status](https://support.garmin.com/en-US/?faq=EjPECQK58qA0xzJ5X74vm7&productID=873214&tab=)

当前只有 FR255，所以它应自然承担两种角色。若 Garmin Connect 仍保留已卖掉的 Venu 3，
应只确认 FR255 已是 Primary Wearable 和 PTD；**不要把旧 Venu 3 重新设为 Primary，
也不需要删除它的历史记录**。旧设备条目是否保留与历史健康数据是否存在是两回事。

### TrueUp 的版本变化

旧论坛（2021 年）有大量用户抱怨 Body Battery 不在 TrueUp 中。该反馈在当时是真实的，
但**不能作为当前结论**。Garmin 现在已有 Body Battery TrueUp，当前兼容列表同时包括
Venu 3 和 FR255。

来源：[当前 Body Battery FAQ](https://support.garmin.com/en-IN/?faq=VOFJAsiXut9K19k1qEn5W5&topicTag=region_bodybatteryfeature)
；历史反馈：[旧版多表不同步讨论](https://forums.garmin.com/sports-fitness/sports-fitness/f/venu/257603/body-battery-not-syncing-with-multiple-watches)

这类变化说明论坛结论必须带日期；设备固件、Connect 和 TrueUp 能力会演进。

## 3. Health Snapshot

### 官方事实

**现役 FR255 在 Health Snapshot 兼容列表中**；历史 Venu 3 也支持。一次会话持续
两分钟，包含心率、HRV、Pulse Ox、呼吸和压力；同步后可在 Connect App/Web 查看并
下载 PDF。

来源：[Garmin Health Snapshot FAQ](https://support.garmin.com/en-SG/?faq=PB1duL5p6V64IQwhNvcRK9)

Garmin Health SDK 能力表也列出 Health Snapshot，但该 SDK/正式 Health API 是合作伙伴
路径，不等于个人账号可自由调用的稳定公共 REST API。

来源：[Garmin Health SDK 概览](https://developer.garmin.com/health-sdk/overview/)

### 用户经验与当前私有 API 限制

- 用户报告同一天手表上记录了两次 Snapshot，但 Connect 只同步出第二次；手工再次
  同步也没恢复第一条。[单次会话漏同步反馈](https://forums.garmin.com/sports-fitness/running-multisport/f/forerunner-970/422207/missing-health-snapshot-forerunner-970-firmware-12-72)
- Instinct 2 用户曾普遍遇到手表提示已上传、Connect 却没有 Snapshot 栏目；Garmin
  员工建立了跟踪案例。[Garmin 跟踪的 Snapshot 显示/同步问题](https://forums.garmin.com/outdoor-recreation/outdoor-recreation/f/instinct-2-series/288538/bug---the-health-image-is-not-displayed-either-on-the-web-garmin-chris)
- 本账号的已知 GraphQL 查询以当前 HA/`ha-garmin` DI Bearer 调用得到 403。这只能说明
  **当前认证方式不能读该私有入口**，不能证明账号没有 Snapshot 数据。

因此后续应优先把“Connect UI/PDF 可见性”作为存在性证据；若要自动归档，再研究正式
合作伙伴接口、完整账户导出或新的可验证私有端点，不应反复撞现有 403 GraphQL。

## 4. ECG

### 官方事实

**FR255 不在 ECG 兼容型号中，因此当前设备不会新增 ECG。** 已卖掉的 Venu 3/Venu 3S
从最低软件版本 7.07 起支持 ECG，台湾在官方支持地区列表中。如果持有 Venu 3 时实际
做过 ECG，历史结果可能仍在账号中；ECG 结果需要启用活动上传和 Data Storage &
Processing 才能同步，Connect App/Web 可以逐条下载 PDF，PDF 也会包含测量时记录的
症状。

来源：[Venu 3 ECG 型号/地区兼容性](https://support.garmin.com/en-IE/?faq=XW4TwGAinJ2juGDNiANMt8&productID=873008&tab=)
；[ECG 同步与 PDF FAQ](https://support.garmin.com/en-SG/?faq=csPQRK57Kz3Jxb2iC9hsy9)

### 用户经验与数据出口

Garmin 官方论坛用户对“完整账户数据导出”做了实际检查：

- 一位用户的导出中出现 `ECG_<uuid>.fit`；
- 另一段目录列表示例包含 `*_SKIN_TEMP.fit`；
- 用户也指出不是每个导出 ZIP 都立即看到 ECG 文件。

来源：[ECG FIT 文件讨论及导出目录示例](https://forums.garmin.com/outdoor-recreation/outdoor-recreation/f/fenix-8-series/421038/meaning-of-fit-fields-in-ecg-fit-files)

这是很有价值的**用户验证**：即使当前 `python-garminconnect` 没有经过验证的 ECG
读取方法，完整账户导出仍可能是安全、低频的归档入口。但文件出现与否取决于账号确实
做过 ECG、数据处理同意、导出范围及 Garmin 的导出实现；不能把论坛样本当作保证。

## 5. 异常心率警报与历史

Garmin 的异常高/低心率警报只有在用户至少静止 10 分钟、心率越过设定阈值时才触发；
它不是持续医疗监护。

来源：[异常心率警报规则](https://support.garmin.com/en-NZ/?faq=y5Ip9aIFKK4CPlQPNcVFu6)

Garmin 员工在较早论坛回复中明确表示，Connect App/Web 当时没有回看异常心率警报
历史的入口；用户则称在申请的账户数据中看到了这些记录。

来源：[异常心率历史论坛讨论](https://forums.garmin.com/sports-fitness/sports-fitness/f/vivosport/165135/abnormal-heart-rate-alert----connect)

本账号接口审计已经看到 `abnormalHeartRateAlertsCount` 和
`abnormalHRValuesArray` 结构，因此后端可能比 UI 暴露更多信息。不过在真实值、
时间戳和事件语义完成低频验证前，应继续把它标为“候选事件历史”，不能仅凭数组名生成
医疗含义。长期分析时保留事件时间和当时全天心率上下文，比只保留每日 count 更有用。

## 6. Move IQ：粒度、用途与误判

### 官方事实

Move IQ 识别步行、跑步、骑行、游泳、椭圆机等熟悉运动模式：

- 需要至少连续 10 分钟；
- 只作为 Connect 日时间线中的事件；
- 不进入普通活动历史、报告或 Newsfeed，也不计入徽章挑战；
- timed activity 的数据更详细、更准确。

来源：[Garmin Move IQ 说明](https://support.garmin.com/en-IN/?faq=zgFpy8MShkArqAxCug5wC6)

### 用户经验

用户报告割草曾被判为骑行，甚至被判为游泳；Garmin 自己的故障排查页也承认重复误判
可能发生，并建议必要时关闭 Move IQ。

来源：[割草误判反馈](https://forums.garmin.com/apps-software/mobile-apps-web/f/garmin-connect-web/131584/move-iq-can-i-delete-a-move-iq-activity)
；[Garmin Move IQ 故障排查](https://support.garmin.com/en-US/?faq=Ne1f3mvRhN5MwCVhLFjdG8)

因此，本账号 `dailyEvents` 出现 `cycling` 只能证明有日事件结构；在核对事件来源字段
前，不能把所有 `dailyEvents` 都标成 Move IQ。即使确认是 Move IQ，也应只保留
`类型 + 起止/时长 + 置信/来源（若有）`，不要虚构 GPS、距离或训练负荷。

## 7. 睡眠、HRV 与 Pulse Ox 缺口

### HRV

Garmin 官方说明，夜间 HRV 即使在全天心率连续时也可能出现断点，因为 HRV 有更严格的
信号质量要求。压住手腕、表带太松、阻碍血流或睡眠中动作较多都会造成缺口。HRV Status
还需要约三周稳定睡眠数据建立个人 baseline；长期不戴会使 baseline 失效。

来源：[HRV 缺口与质量要求](https://support.garmin.com/en-US/?faq=04pnPSBTYSAYL9FylZoUl5)
；[HRV Status 三周基线](https://support.garmin.com/en-US/?faq=HnFAR4oFRF4kHeqYme3bU6&productID=742133&tab=topics)

用户也报告过“手表有 HRV、Connect App/Web 某一晚为空”的同步型故障；这类情况与
传感器根本没记录要分开诊断。

来源：[HRV 已记录但未同步的用户反馈](https://forums.garmin.com/apps-software/mobile-apps-web/f/garmin-connect-mobile-andriod/334266/nightly-hrv-status-did-not-record-and-has-disappeared-from-my-available-cards)

### Pulse Ox

Pulse Ox 是否有完整序列首先取决于手表设置：

- All-Day 模式通常每 5–15 分钟采样，活动较多时更少；
- Sleep 模式可高至约每分钟一次；
- 低电量时连续采样不会启动；
- 动作、佩戴松紧、皮肤接触和血流都会影响读数；
- 因此必须核对现役 FR255 的 Pulse Ox 模式，不能仅凭某日数组为空推断设备不支持。

来源：[Garmin Pulse Ox FAQ](https://support.garmin.com/en-CA/?faq=SK2Y9a9aBp5D6n4sXmPBG7)

### 睡眠

睡眠数据要求光学心率开启、睡眠时间窗正确、表带合适，且 Battery Saver 不能关闭所需
传感器；多设备用户还应把夜间佩戴设备设为 Primary Wearable。

来源：[Garmin 睡眠准确性/设置要求](https://support.garmin.com/en-US/?faq=qvzNMwxuTb9NxZ6Ce2a9z9)

Garmin 员工曾确认并修复“手表有睡眠，Connect 不显示”的服务/同步问题。因此处理
历史序列时，`null`/空数组必须保留为“缺测”，不能写成 0，也不应立即归因于个人生理。

来源：[Garmin 确认的睡眠显示故障](https://forums.garmin.com/apps-software/mobile-apps-web/f/garmin-connect-mobile-ios/274922/sleep-informations-not-visible-on-the-app/1318387)

## 8. 旧历史数据、空数组与回补

Garmin 官方把 wellness 数据缺失的常见原因列为：设备关联过多个 Connect 账号、
App/Web 登录不同账号、活动追踪关闭、时区或日期错误，以及没有同意上传。

来源：[Garmin wellness 数据缺失排查](https://support.garmin.com/en-US/?faq=fqrQCPrJsx7UXoQM7FxCBA)

`python-garminconnect` 还提供 `wellness/epoch/request/{date}`，其源码注释说明可请求
Garmin 重新加载已 offload 的较老日期数据。这是私有 Connect API 行为，没有稳定性
保证，但说明“旧日期返回空数组”不必然等于历史从未存在。

来源：[旧 wellness 数据 reload 方法](https://github.com/cyberjunky/python-garminconnect/blob/206876670d73eb9749674bfa3c3ec67bfa3b77b4/garminconnect/__init__.py#L2735-L2743)

GarminDB 的设计也侧面说明长期归档不能依赖每次全量重拉：它先保存下载的原始文件，
数据库 schema 更新时从本地文件重建，不重新下载全部 Garmin 数据；同时用统计摘要
检查各数据族的日期覆盖。

来源：[GarminDB README](https://github.com/tcgoetz/GarminDB)

对本项目最稳妥的语义是：

- `0`：Garmin 明确报告测量值为零；
- `null`/空数组：该接口在该日没有返回点，原因未知；
- `unsupported`：有官方设备能力或明确 schema 证据证明不支持；
- `not_requested`：尚未请求；
- `offloaded_pending`：已触发一次旧数据 reload，等待后续低频复查；
- `confirmed_absent`：Connect UI/导出与接口都没有，且已排除设置/账号/同步问题。

## 9. 非官方 Connect API 限流

### 已知证据

`python-garminconnect` #337 的 429 堆栈明确落在：

`/oauth-service/oauth/preauthorized`

也就是 OAuth 登录完成阶段，不是 HR、睡眠、HRV 等普通数据读取端点。另一份 garth
issue 同期也报告 `/mobile/api/login` 429。两者说明反复创建登录会话尤其危险。

来源：[python-garminconnect #337](https://github.com/cyberjunky/python-garminconnect/issues/337)
；[garth #217 issue 列表](https://github.com/matin/garth/issues)

当前 `python-garminconnect` 对 429 明确 fail fast，不把它当作可自动重试错误；只对
5xx 和网络故障做带抖动的指数退避。

来源：[当前请求错误处理源码](https://github.com/cyberjunky/python-garminconnect/blob/master/garminconnect/__init__.py)

Home Assistant Garmin 集成当前文档给出的保守边界是最小 60 秒轮询；遇到 429 时等待
5–30 分钟再加载集成。该数值是项目维护策略，不是 Garmin 官方公布的账号配额。

来源：[HA Garmin 集成 Known Limitations / Rate limit](https://github.com/cyberjunky/home-assistant-garmin_connect)

### 对后续实现的约束

1. 持久化并复用 OAuth token，正常增量同步不得重新执行用户名/密码 SSO。
2. 端点按数据变化速度分层；睡眠、皮温、训练状态无需分钟级读取。
3. 用 `端点 + 日期 + 参数` 完成账本去重，已经成功的历史日期默认不再请求。
4. 429、401、403 都立即停止对应路径；429 不自动重试，401 不循环重新登录，
   403 不换参数反复撞同一私有入口。
5. 记录响应状态、结构和点数即可；日志不保存凭据、token 或完整健康 payload。
6. 精确限流阈值未知，不能因为一次 30 秒间隔审计未触发 429 就把 30 秒当安全配额。

## 10. 建议的下一步验证顺序

这不是轮询计划，只是后续单次验证的优先顺序：

1. 在 Garmin Connect 核对 **FR255** 已是 Primary Wearable 和 PTD；旧 Venu 3
   即使仍列在设备中，也不要再设为 Primary。
2. 在 Connect UI 确认 FR255 的 Health Snapshot 是否已有记录；FR255 支持该功能。
3. ECG 与皮温只盘点 **Venu 3 持有期间的历史**，不要期待 FR255 产生新数据。
4. 若需要 Venu 3 的 ECG/皮温历史，先做一次 Garmin 官方完整账户导出，盘点文件名
   和日期覆盖，不先猜私有 URL。
5. 对旧日期空数组，只挑一个 Connect UI 已确认有数据的日期做一次 reload/复查，
   不做大范围回填。
6. 后续数据库把“缺测原因状态”与数值分开保存，避免把空数组画成 0。
