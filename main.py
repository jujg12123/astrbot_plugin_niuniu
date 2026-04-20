import random
import yaml
import os
import re
import time
import json
import sys
import asyncio
from datetime import datetime, timedelta
import pytz

from astrbot.api.all import *

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from niuniu_shop import NiuniuShop
from niuniu_games import NiuniuGames

# ========== 常量定义 ==========
PLUGIN_DIR = os.path.join('data', 'plugins', 'astrbot_plugin_niuniu')
os.makedirs(PLUGIN_DIR, exist_ok=True)

NIUNIU_LENGTHS_FILE = os.path.join('data', 'niuniu_lengths.yml')
NIUNIU_TEXTS_FILE = os.path.join(PLUGIN_DIR, 'niuniu_game_texts.yml')
LAST_ACTION_FILE = os.path.join(PLUGIN_DIR, 'last_actions.yml')
PURCHASE_DATA_FILE = os.path.join(PLUGIN_DIR, 'purchase_counts.yml')
MARKET_FILE = os.path.join(PLUGIN_DIR, 'market_listings.yml')  # 新增市场数据

WEALTH_LEVELS = [
    (0, "平民", 0.25),
    (500, "小资", 0.5),
    (2000, "富豪", 0.75),
    (5000, "巨擘", 1.0),
]
WEALTH_BASE_VALUES = {"平民": 100.0, "小资": 500.0, "富豪": 2000.0, "巨擘": 5000.0}
BASE_INCOME = 100.0
SHANGHAI_TZ = pytz.timezone("Asia/Shanghai")
INTEREST_RATE_PER_MINUTE = 0.001
EMPLOYEE_EARNINGS_RATE = 0.01  # 雇员每分钟收益 = 身价 * 0.01


@register("niuniu_plugin", "长安某", "牛牛插件 v5.3.0（市场、排行奖励、撅）", "5.3.0")
class NiuniuPlugin(Star):
    COOLDOWN_10_MIN = 600
    COOLDOWN_30_MIN = 1800
    COMPARE_COOLDOWN = 600

    def __init__(self, context: Context, config: dict = None):
        super().__init__(context)
        self.config = config or {}
        self.niuniu_texts = self._load_niuniu_texts()
        self.last_actions = self._load_last_actions()
        self.admins = self._load_admins()
        self.shop = NiuniuShop(self)
        self.games = NiuniuGames(self)
        self.purchase_data = {}
        self.market_listings = []  # 市场挂单列表
        self._processed_messages = set()
        self._max_processed_cache = 2000
        asyncio.create_task(self._async_init())

    async def _async_init(self):
        data = self._load_niuniu_lengths()
        migrated = self._migrate_all_data(data)
        if migrated:
            self._save_niuniu_lengths(data)
        await self._load_purchase_data()
        await self._load_market_data()
        # 启动每日排行奖励定时器
        asyncio.create_task(self._daily_ranking_reward_task())
        self.context.logger.info("牛牛插件 v5.3.0 初始化完成")

    # ========== 数据迁移（新增 employee_earnings_last_time、market字段） ==========
    def _migrate_user_data(self, user_data: dict) -> dict:
        if 'coins' in user_data:
            if isinstance(user_data['coins'], int):
                user_data['coins'] = float(user_data['coins'])
        else:
            user_data['coins'] = 0.0
        user_data.setdefault('bank', 0.0)
        user_data.setdefault('contractors', [])
        user_data.setdefault('contracted_by', None)
        user_data.setdefault('last_sign', None)
        user_data.setdefault('consecutive', 0)
        user_data.setdefault('nickname', '')
        user_data.setdefault('length', 5)
        user_data.setdefault('hardness', 1)
        user_data.setdefault('items', {})
        user_data.setdefault('last_interest_time', None)
        user_data.setdefault('employee_earnings', {})
        user_data.setdefault('employee_earnings_last_time', None)  # 新增
        return user_data

    def _migrate_all_data(self, data: dict) -> bool:
        modified = False
        for group_id, group_data in data.items():
            if not isinstance(group_data, dict):
                continue
            for user_id, user_data in list(group_data.items()):
                if user_id == 'plugin_enabled':
                    continue
                if isinstance(user_data, dict):
                    old_keys = set(user_data.keys())
                    self._migrate_user_data(user_data)
                    if old_keys != set(user_data.keys()):
                        modified = True
        return modified

    # ========== 数据文件操作 ==========
    def _load_niuniu_lengths(self):
        if not os.path.exists(NIUNIU_LENGTHS_FILE):
            self._create_niuniu_lengths_file()
        try:
            with open(NIUNIU_LENGTHS_FILE, 'r', encoding='utf-8') as f:
                data = yaml.safe_load(f) or {}
            for group_id in list(data.keys()):
                group_data = data[group_id]
                if not isinstance(group_data, dict):
                    data[group_id] = {'plugin_enabled': False}
                elif 'plugin_enabled' not in group_data:
                    group_data['plugin_enabled'] = False
            return data
        except Exception as e:
            self.context.logger.error(f"加载数据失败: {str(e)}")
            return {}

    def _create_niuniu_lengths_file(self):
        try:
            with open(NIUNIU_LENGTHS_FILE, 'w', encoding='utf-8') as f:
                yaml.dump({}, f)
        except Exception as e:
            self.context.logger.error(f"创建文件失败: {str(e)}")

    def _save_niuniu_lengths(self, data):
        try:
            with open(NIUNIU_LENGTHS_FILE, 'w', encoding='utf-8') as f:
                yaml.dump(data, f, allow_unicode=True)
        except Exception as e:
            self.context.logger.error(f"保存失败: {str(e)}")

    async def _load_purchase_data(self):
        try:
            with open(PURCHASE_DATA_FILE, 'r', encoding='utf-8') as f:
                self.purchase_data = yaml.safe_load(f.read()) or {}
        except FileNotFoundError:
            self.purchase_data = {}
        except Exception as e:
            self.context.logger.error(f"加载雇佣次数失败: {e}")
            self.purchase_data = {}

    async def _save_purchase_data(self):
        try:
            with open(PURCHASE_DATA_FILE, 'w', encoding='utf-8') as f:
                f.write(yaml.dump(self.purchase_data, allow_unicode=True))
        except Exception as e:
            self.context.logger.error(f"保存雇佣次数失败: {e}")

    async def _load_market_data(self):
        try:
            if os.path.exists(MARKET_FILE):
                with open(MARKET_FILE, 'r', encoding='utf-8') as f:
                    self.market_listings = yaml.safe_load(f.read()) or []
            else:
                self.market_listings = []
        except Exception as e:
            self.context.logger.error(f"加载市场数据失败: {e}")
            self.market_listings = []

    async def _save_market_data(self):
        try:
            with open(MARKET_FILE, 'w', encoding='utf-8') as f:
                f.write(yaml.dump(self.market_listings, allow_unicode=True))
        except Exception as e:
            self.context.logger.error(f"保存市场数据失败: {e}")

    # ========== 文本配置加载 ==========
    def _load_niuniu_texts(self):
        default_texts = {
            'register': {
                'success': "🧧 {nickname} 成功注册牛牛！\n📏 初始长度：{length}cm\n💪 硬度等级：{hardness}",
                'already_registered': "⚠️ {nickname} 你已经注册过牛牛啦！",
            },
            'dajiao': {
                'cooldown': ["⏳ {nickname} 牛牛需要休息，{remaining}分钟后可再打胶"],
                'increase': ["🚀 {nickname} 打胶成功！长度增加 {change}cm！"],
                'decrease': ["😱 {nickname} 用力过猛！长度减少 {change}cm！"],
                'decrease_30min': ["😱 {nickname} 用力过猛！长度减少 {change}cm！"],
                'no_effect': ["🌀 {nickname} 的牛牛毫无变化..."],
                'not_registered': "❌ {nickname} 请先注册牛牛"
            },
            'my_niuniu': {
                'info': "📊 {nickname} 的牛牛状态\n📏 长度：{length}\n💪 硬度：{hardness}\n📝 评价：{evaluation}",
                'evaluation': {
                    'short': ["小巧玲珑"], 'medium': ["中规中矩"], 'long': ["威风凛凛"],
                    'very_long': ["擎天巨柱"], 'super_long': ["超级长"], 'ultra_long': ["超越极限"]
                },
                'not_registered': "❌ {nickname} 请先注册牛牛"
            },
            'compare': {
                'no_target': "❌ {nickname} 请指定比划对象",
                'target_not_registered': "❌ 对方尚未注册牛牛",
                'cooldown': "⏳ {nickname} 请等待{remaining}分钟后再比划",
                'self_compare': "❌ 不能和自己比划",
                'win': ["🎉 {winner} 战胜了 {loser}！\n📈 增加 {gain}cm"],
                'lose': ["😭 {loser} 败给 {winner}\n📉 减少 {loss}cm"],
                'draw': "🤝 双方势均力敌！",
                'double_loss': "😱 {nickname1} 和 {nickname2} 的牛牛因过于柔软发生缠绕，长度减半！",
                'user_no_increase': "😅 {nickname} 的牛牛没有任何增长。"
            },
            'ranking': {
                'header': "🏅 牛牛排行榜 TOP10：\n",
                'no_data': "📭 本群暂无牛牛数据",
                'item': "{rank}. {name} ➜ {length}"
            },
            'menu': {
                'default': """📜 牛牛菜单 v5.3.0：
🔹 注册牛牛 - 初始化
🔹 打胶 - 提升长度
🔹 开冲/停止开冲/飞飞机 - 赚金币
🔹 我的牛牛 - 查看状态
🔹 比划比划 @目标 - 对决
🔹 牛牛排行 - 群排行榜
🔹 牛牛商城/牛牛购买/牛牛背包 - 道具
🔹 签到 - 每日签到
🔹 存款/取款 <金额> - 银行
🔹 查询银行/领取利息
🔹 转账 @目标 <金额>
🔹 购买/出售 @目标 - 雇佣
🔹 赎身 - 重获自由
🔹 我的雇员/领取雇员收益
🔹 排行榜/财富榜 - 财富排行
🔹 我的信息 - 签到状态
🔹 出售牛牛 <长度> <价格> - 挂单市场
🔹 牛牛市场 - 查看挂单
🔹 牛牛市场购买 <编号>
🔹 撅 @目标 - 付100金币夺取长度
🔹 牛牛开/关 - 管理插件"""
            },
            'system': {
                'enable': "✅ 牛牛插件已启用",
                'disable': "❌ 牛牛插件已禁用"
            }
        }
        try:
            if os.path.exists(NIUNIU_TEXTS_FILE):
                with open(NIUNIU_TEXTS_FILE, 'r', encoding='utf-8') as f:
                    custom_texts = yaml.safe_load(f) or {}
                    return self._deep_merge(default_texts, custom_texts)
        except Exception as e:
            self.context.logger.error(f"加载文本失败: {str(e)}")
        return default_texts

    def _deep_merge(self, base, update):
        for key, value in update.items():
            if isinstance(value, dict):
                base[key] = self._deep_merge(base.get(key, {}), value)
            else:
                base[key] = value
        return base

    def _load_last_actions(self):
        try:
            with open(LAST_ACTION_FILE, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f) or {}
        except:
            return {}

    def _save_last_actions(self, data):
        try:
            with open(LAST_ACTION_FILE, 'w', encoding='utf-8') as f:
                yaml.dump(data, f, allow_unicode=True)
        except Exception as e:
            self.context.logger.error(f"保存冷却数据失败: {str(e)}")

    def _load_admins(self):
        try:
            with open(os.path.join('data', 'cmd_config.json'), 'r', encoding='utf-8-sig') as f:
                config = json.load(f)
                return config.get('admins_id', [])
        except:
            return []

    def is_admin(self, user_id):
        return str(user_id) in self.admins

    def get_group_data(self, group_id):
        group_id = str(group_id)
        data = self._load_niuniu_lengths()
        if group_id not in data:
            data[group_id] = {'plugin_enabled': False}
            self._save_niuniu_lengths(data)
        return data[group_id]

    def get_user_data(self, group_id, user_id):
        group_id = str(group_id)
        user_id = str(user_id)
        data = self._load_niuniu_lengths()
        group_data = data.get(group_id, {'plugin_enabled': False})
        return group_data.get(user_id)

    def update_user_data(self, group_id, user_id, updates):
        group_id = str(group_id)
        user_id = str(user_id)
        data = self._load_niuniu_lengths()
        group_data = data.setdefault(group_id, {'plugin_enabled': False})
        user_data = group_data.setdefault(user_id, {
            'nickname': '', 'length': 0, 'hardness': 1,
            'coins': 0.0, 'bank': 0.0, 'contractors': [],
            'contracted_by': None, 'last_sign': None, 'consecutive': 0,
            'items': {}, 'last_interest_time': None,
            'employee_earnings': {}, 'employee_earnings_last_time': None
        })
        user_data.update(updates)
        self._save_niuniu_lengths(data)
        return user_data

    def update_group_data(self, group_id, updates):
        group_id = str(group_id)
        data = self._load_niuniu_lengths()
        group_data = data.setdefault(group_id, {'plugin_enabled': False})
        group_data.update(updates)
        self._save_niuniu_lengths(data)
        return group_data

    def format_length(self, length):
        if length >= 100:
            return f"{length/100:.2f}m"
        return f"{length}cm"

    def check_cooldown(self, last_time, cooldown):
        current = time.time()
        elapsed = current - last_time
        remaining = cooldown - elapsed
        return remaining > 0, remaining

    def parse_at_target(self, event):
        for comp in event.message_obj.message:
            if isinstance(comp, At):
                return str(comp.qq)
        return None

    def parse_target(self, event):
        for comp in event.message_obj.message:
            if isinstance(comp, At):
                return str(comp.qq)
        msg = event.message_str.strip()
        if msg.startswith("比划比划"):
            target_name = msg[len("比划比划"):].strip()
            if target_name:
                group_id = str(event.message_obj.group_id)
                group_data = self.get_group_data(group_id)
                for user_id, user_data in group_data.items():
                    if isinstance(user_data, dict) and user_data.get('nickname'):
                        if re.search(re.escape(target_name), user_data['nickname'], re.IGNORECASE):
                            return user_id
        return None

    # ========== 每日排行奖励定时任务 ==========
    async def _daily_ranking_reward_task(self):
        while True:
            now = datetime.now(SHANGHAI_TZ)
            # 计算下一个6:00
            next_run = now.replace(hour=6, minute=0, second=0, microsecond=0)
            if now >= next_run:
                next_run += timedelta(days=1)
            wait_seconds = (next_run - now).total_seconds()
            await asyncio.sleep(wait_seconds)
            await self._give_daily_ranking_rewards()

    async def _give_daily_ranking_rewards(self):
        data = self._load_niuniu_lengths()
        for group_id, group_data in data.items():
            if not isinstance(group_data, dict) or not group_data.get('plugin_enabled', False):
                continue
            valid_users = [(uid, u_data) for uid, u_data in group_data.items()
                           if isinstance(u_data, dict) and 'length' in u_data]
            if not valid_users:
                continue
            sorted_users = sorted(valid_users, key=lambda x: x[1]['length'], reverse=True)[:10]
            for idx, (uid, u_data) in enumerate(sorted_users):
                rank = idx + 1
                reward = max(100 - (rank - 1) * 10, 10)  # 1st:100, 2nd:90, ..., 9th:20, 10th:10
                if rank == 10:
                    reward += 100  # 第十名额外100
                u_data['coins'] = u_data.get('coins', 0.0) + reward
                # 可以尝试发送私聊通知，此处简化仅加钱
            self._save_niuniu_lengths(data)
        self.context.logger.info("每日排行奖励已发放")

    # ========== 事件处理（含消息去重） ==========
    @event_message_type(EventMessageType.GROUP_MESSAGE)
    async def on_group_message(self, event: AstrMessageEvent):
        # 消息去重
        msg_id = None
        try:
            if hasattr(event.message_obj, 'message_id'):
                msg_id = event.message_obj.message_id
            elif hasattr(event.message_obj, 'raw_message_id'):
                msg_id = event.message_obj.raw_message_id
            else:
                msg_id = f"{event.message_obj.group_id}_{event.get_sender_id()}_{hash(event.message_str)}"
        except:
            msg_id = f"{event.message_obj.group_id}_{event.get_sender_id()}_{time.time()}"
        if msg_id in self._processed_messages:
            return
        self._processed_messages.add(msg_id)
        if len(self._processed_messages) > self._max_processed_cache:
            self._processed_messages.clear()

        group_id = str(event.message_obj.group_id)
        group_data = self.get_group_data(group_id)
        msg = event.message_str.strip()

        # 开关与菜单
        if msg.startswith("牛牛开"):
            async for result in self._toggle_plugin(event, True):
                yield result
            return
        elif msg.startswith("牛牛关"):
            async for result in self._toggle_plugin(event, False):
                yield result
            return
        elif msg.startswith("牛牛菜单"):
            async for result in self._show_menu(event):
                yield result
            return

        if not group_data.get('plugin_enabled', False):
            return

        user_id = str(event.get_sender_id())
        user_data = self.get_user_data(group_id, user_id)
        is_rushing = user_data.get('is_rushing', False) if user_data else False

        # 开冲系列
        if msg.startswith("开冲"):
            if is_rushing:
                yield event.plain_result("❌ 你已经在开冲了")
                return
            async for result in self.games.start_rush(event):
                yield result
            return
        elif msg.startswith("停止开冲"):
            if not is_rushing:
                yield event.plain_result("❌ 你当前并未在开冲")
                return
            async for result in self.games.stop_rush(event):
                yield result
            return
        elif msg.startswith("飞飞机"):
            if is_rushing:
                yield event.plain_result("❌ 牛牛快冲晕了，还做不了其他事情，要不先停止开冲？")
                return
            async for result in self.games.fly_plane(event):
                yield result
            return

        # 签到系统命令
        if msg.startswith("签到"):
            if is_rushing:
                yield event.plain_result("❌ 牛牛快冲晕了，还做不了其他事情，要不先停止开冲？")
                return
            async for result in self.sign_in(event):
                yield result
            return
        elif msg.startswith("存款") or msg.startswith("存钱"):
            if is_rushing:
                yield event.plain_result("❌ 牛牛快冲晕了，还做不了其他事情，要不先停止开冲？")
                return
            parts = msg.split()
            if len(parts) >= 2:
                async for result in self.deposit(event, parts[1]):
                    yield result
            else:
                yield event.plain_result("请指定存款金额，例如：存款 100")
            return
        elif msg.startswith("取款") or msg.startswith("取钱"):
            if is_rushing:
                yield event.plain_result("❌ 牛牛快冲晕了，还做不了其他事情，要不先停止开冲？")
                return
            parts = msg.split()
            if len(parts) >= 2:
                async for result in self.withdraw(event, parts[1]):
                    yield result
            else:
                yield event.plain_result("请指定取款金额，例如：取款 100")
            return
        elif msg.startswith("查询银行") or msg.startswith("银行信息"):
            if is_rushing:
                yield event.plain_result("❌ 牛牛快冲晕了，还做不了其他事情，要不先停止开冲？")
                return
            async for result in self.bank_info(event):
                yield result
            return
        elif msg.startswith("领取利息"):
            if is_rushing:
                yield event.plain_result("❌ 牛牛快冲晕了，还做不了其他事情，要不先停止开冲？")
                return
            async for result in self.claim_interest(event):
                yield result
            return
        elif msg.startswith("转账"):
            if is_rushing:
                yield event.plain_result("❌ 牛牛快冲晕了，还做不了其他事情，要不先停止开冲？")
                return
            target_id = self.parse_at_target(event)
            parts = msg.split()
            if target_id and len(parts) >= 2:
                amount_str = parts[-1]
                async for result in self.transfer(event, target_id, amount_str):
                    yield result
            else:
                yield event.plain_result("格式：转账 @目标 金额")
            return
        elif msg.startswith("购买"):
            if is_rushing:
                yield event.plain_result("❌ 牛牛快冲晕了，还做不了其他事情，要不先停止开冲？")
                return
            async for result in self.purchase_contractor(event):
                yield result
            return
        elif msg.startswith("出售"):
            if is_rushing:
                yield event.plain_result("❌ 牛牛快冲晕了，还做不了其他事情，要不先停止开冲？")
                return
            if msg.startswith("出售牛牛"):
                async for result in self.sell_length_market(event, msg):
                    yield result
            else:
                async for result in self.sell_contractor(event):
                    yield result
            return
        elif msg.startswith("赎身"):
            if is_rushing:
                yield event.plain_result("❌ 牛牛快冲晕了，还做不了其他事情，要不先停止开冲？")
                return
            async for result in self.terminate_contract(event):
                yield result
            return
        elif msg.startswith("我的雇员") or msg.startswith("雇员列表"):
            if is_rushing:
                yield event.plain_result("❌ 牛牛快冲晕了，还做不了其他事情，要不先停止开冲？")
                return
            async for result in self.show_employees(event):
                yield result
            return
        elif msg.startswith("领取雇员收益"):
            if is_rushing:
                yield event.plain_result("❌ 牛牛快冲晕了，还做不了其他事情，要不先停止开冲？")
                return
            async for result in self.claim_employee_earnings(event):
                yield result
            return
        elif msg.startswith("牛牛市场购买"):
            if is_rushing:
                yield event.plain_result("❌ 牛牛快冲晕了，还做不了其他事情，要不先停止开冲？")
                return
            parts = msg.split()
            if len(parts) >= 2:
                async for result in self.buy_from_market(event, parts[-1]):
                    yield result
            else:
                yield event.plain_result("格式：牛牛市场购买 <编号>")
            return
        elif msg.startswith("牛牛市场"):
            if is_rushing:
                yield event.plain_result("❌ 牛牛快冲晕了，还做不了其他事情，要不先停止开冲？")
                return
            async for result in self.show_market(event):
                yield result
            return
        elif msg.startswith("撅"):
            if is_rushing:
                yield event.plain_result("❌ 牛牛快冲晕了，还做不了其他事情，要不先停止开冲？")
                return
            target_id = self.parse_at_target(event)
            if target_id:
                async for result in self.jue(event, target_id):
                    yield result
            else:
                yield event.plain_result("请@要撅的对象")
            return
        elif msg.startswith("排行榜") or msg.startswith("财富榜"):
            if is_rushing:
                yield event.plain_result("❌ 牛牛快冲晕了，还做不了其他事情，要不先停止开冲？")
                return
            async for result in self.wealth_leaderboard(event):
                yield result
            return
        elif msg.startswith("我的信息") or msg.startswith("签到查询") or msg.startswith("我的资产"):
            if is_rushing:
                yield event.plain_result("❌ 牛牛快冲晕了，还做不了其他事情，要不先停止开冲？")
                return
            async for result in self.sign_query(event):
                yield result
            return

        # 牛牛原有命令
        handler_map = {
            "注册牛牛": self._register,
            "打胶": self._dajiao,
            "我的牛牛": self._show_status,
            "比划比划": self._compare,
            "牛牛排行": self._show_ranking,
            "牛牛商城": self.shop.show_shop,
            "牛牛购买": self.shop.handle_buy,
            "牛牛背包": self.shop.show_items
        }
        for cmd, handler in handler_map.items():
            if msg.startswith(cmd):
                if is_rushing:
                    yield event.plain_result("❌ 牛牛快冲晕了，还做不了其他事情，要不先停止开冲？")
                    return
                async for result in handler(event):
                    yield result
                return

    @event_message_type(EventMessageType.PRIVATE_MESSAGE)
    async def on_private_message(self, event: AstrMessageEvent):
        msg = event.message_str.strip()
        niuniu_commands = [
            "牛牛菜单", "牛牛开", "牛牛关", "注册牛牛", "打胶", "我的牛牛",
            "比划比划", "牛牛排行", "牛牛商城", "牛牛购买", "牛牛背包",
            "开冲", "停止开冲", "飞飞机", "签到", "存款", "取款", "转账", "购买",
            "出售", "出售牛牛", "赎身", "排行榜", "财富榜", "我的信息", "签到查询", "我的资产",
            "查询银行", "银行信息", "领取利息", "我的雇员", "雇员列表", "领取雇员收益",
            "牛牛市场", "牛牛市场购买", "撅"
        ]
        if any(msg.startswith(cmd) for cmd in niuniu_commands):
            yield event.plain_result("不许一个人偷偷玩牛牛")

    # ========== 牛牛原有方法 ==========
    async def _toggle_plugin(self, event, enable):
        group_id = str(event.message_obj.group_id)
        user_id = str(event.get_sender_id())
        if not self.is_admin(user_id):
            yield event.plain_result("❌ 只有管理员才能使用此指令")
            return
        self.update_group_data(group_id, {'plugin_enabled': enable})
        text_key = 'enable' if enable else 'disable'
        yield event.plain_result(self.niuniu_texts['system'][text_key])

    async def _register(self, event):
        group_id = str(event.message_obj.group_id)
        user_id = str(event.get_sender_id())
        nickname = event.get_sender_name()
        if self.get_user_data(group_id, user_id):
            text = self.niuniu_texts['register']['already_registered'].format(nickname=nickname)
            yield event.plain_result(text)
            return
        cfg = self.config.get('niuniu_config', {})
        user_data = {
            'nickname': nickname,
            'length': random.randint(cfg.get('min_length', 3), cfg.get('max_length', 10)),
            'hardness': 1,
            'coins': 0.0, 'bank': 0.0, 'contractors': [],
            'contracted_by': None, 'last_sign': None, 'consecutive': 0, 'items': {},
            'last_interest_time': None, 'employee_earnings': {}, 'employee_earnings_last_time': None
        }
        self.update_user_data(group_id, user_id, user_data)
        text = self.niuniu_texts['register']['success'].format(
            nickname=nickname, length=user_data['length'], hardness=user_data['hardness']
        )
        yield event.plain_result(text)

    async def _dajiao(self, event: AstrMessageEvent):
        group_id = str(event.message_obj.group_id)
        user_id = str(event.get_sender_id())
        nickname = event.get_sender_name()
        user_data = self.get_user_data(group_id, user_id)
        if not user_data:
            text = self.niuniu_texts['dajiao']['not_registered'].format(nickname=nickname)
            yield event.plain_result(text)
            return
        user_items = self.shop.get_user_items(group_id, user_id)
        has_zhiming_rhythm = user_items.get("致命节奏", 0) > 0
        last_actions = self._load_last_actions()
        last_time = last_actions.setdefault(group_id, {}).get(user_id, {}).get('dajiao', 0)
        on_cooldown, remaining = self.check_cooldown(last_time, self.COOLDOWN_10_MIN)
        result_msg = []
        if on_cooldown and has_zhiming_rhythm:
            self.shop.consume_item(group_id, user_id, "致命节奏")
            result_msg.append(f"⚡ 触发致命节奏！{nickname} 无视冷却强行打胶！")
            elapsed = self.COOLDOWN_30_MIN + 1
        else:
            if on_cooldown and not has_zhiming_rhythm:
                mins = int(remaining // 60) + 1
                text = random.choice(self.niuniu_texts['dajiao']['cooldown']).format(
                    nickname=nickname, remaining=mins
                )
                yield event.plain_result(text)
                return
            elapsed = time.time() - last_time
        change = 0
        current_time = time.time()
        if elapsed < self.COOLDOWN_30_MIN:
            rand = random.random()
            if rand < 0.4:
                change = random.randint(2, 5)
            elif rand < 0.7:
                change = -random.randint(1, 3)
                template = random.choice(self.niuniu_texts['dajiao']['decrease'])
        else:
            rand = random.random()
            if rand < 0.7:
                change = random.randint(3, 6)
                user_data['hardness'] = min(user_data.get('hardness', 1) + 1, 10)
            elif rand < 0.9:
                change = -random.randint(1, 2)
                template = random.choice(self.niuniu_texts['dajiao']['decrease_30min'])
        updated_data = {'length': max(1, user_data['length'] + change)}
        if 'hardness' in locals():
            updated_data['hardness'] = user_data['hardness']
        self.update_user_data(group_id, user_id, updated_data)
        last_actions = self._load_last_actions()
        last_actions.setdefault(group_id, {}).setdefault(user_id, {})['dajiao'] = current_time
        self._save_last_actions(last_actions)
        if change > 0:
            template = random.choice(self.niuniu_texts['dajiao']['increase'])
        elif change == 0:
            template = random.choice(self.niuniu_texts['dajiao']['no_effect'])
        text = template.format(nickname=nickname, change=abs(change))
        if result_msg:
            final_text = "\n".join(result_msg + [text])
        else:
            final_text = text
        user_data = self.get_user_data(group_id, user_id)
        yield event.plain_result(f"{final_text}\n当前长度：{self.format_length(user_data['length'])}")

    async def _compare(self, event):
        group_id = str(event.message_obj.group_id)
        user_id = str(event.get_sender_id())
        nickname = event.get_sender_name()
        user_data = self.get_user_data(group_id, user_id)
        if not user_data:
            yield event.plain_result(self.niuniu_texts['dajiao']['not_registered'].format(nickname=nickname))
            return
        target_id = self.parse_target(event)
        if not target_id:
            yield event.plain_result(self.niuniu_texts['compare']['no_target'].format(nickname=nickname))
            return
        if target_id == user_id:
            yield event.plain_result(self.niuniu_texts['compare']['self_compare'])
            return
        target_data = self.get_user_data(group_id, target_id)
        if not target_data:
            yield event.plain_result(self.niuniu_texts['compare']['target_not_registered'])
            return

        # 冷却检查
        last_actions = self._load_last_actions()
        compare_records = last_actions.setdefault(group_id, {}).setdefault(user_id, {})
        last_compare = compare_records.get(target_id, 0)
        on_cooldown, remaining = self.check_cooldown(last_compare, self.COMPARE_COOLDOWN)
        if on_cooldown:
            mins = int(remaining // 60) + 1
            text = self.niuniu_texts['compare']['cooldown'].format(nickname=nickname, remaining=mins)
            yield event.plain_result(text)
            return
        last_compare_time = compare_records.get('last_time', 0)
        current_time = time.time()
        if current_time - last_compare_time > 600:
            compare_records['count'] = 0
            compare_records['last_time'] = current_time
            self._save_last_actions(last_actions)
        compare_count = compare_records.get('count', 0)
        if compare_count >= 3:
            yield event.plain_result("❌ 10分钟内只能比划三次")
            return
        compare_records[target_id] = current_time
        compare_records['count'] = compare_count + 1
        self._save_last_actions(last_actions)

        # 道具：夺心魔蝌蚪罐头
        user_items = self.shop.get_user_items(group_id, user_id)
        if user_items.get("夺心魔蝌蚪罐头", 0) > 0:
            effect_chance = random.random()
            if effect_chance < 0.5:
                original_target_length = target_data['length']
                updated_user = {'length': user_data['length'] + original_target_length}
                updated_target = {'length': 1}
                self.update_user_data(group_id, user_id, updated_user)
                self.update_user_data(group_id, target_id, updated_target)
                result_msg = [
                    "⚔️ 【牛牛对决结果】 ⚔️",
                    f"🎉 {nickname} 获得了夺心魔技能，夺取了 {target_data['nickname']} 的全部长度！",
                    f"🗡️ {nickname}: {self.format_length(user_data['length'] - original_target_length)} → {self.format_length(user_data['length'] + original_target_length)}",
                    f"🛡️ {target_data['nickname']}: {self.format_length(original_target_length)} → 1cm"
                ]
                self.shop.consume_item(group_id, user_id, "夺心魔蝌蚪罐头")
                yield event.plain_result("\n".join(result_msg))
                return
            elif effect_chance < 0.6:
                updated_user = {'length': 1}
                self.update_user_data(group_id, user_id, updated_user)
                result_msg = [
                    "⚔️ 【牛牛对决结果】 ⚔️",
                    f"💔 {nickname} 使用夺心魔蝌蚪罐头，牛牛变成了夺心魔！！！",
                    f"🗡️ {nickname}: {self.format_length(user_data['length'])} → 1cm",
                    f"🛡️ {target_data['nickname']}: {self.format_length(target_data['length'])}"
                ]
                self.shop.consume_item(group_id, user_id, "夺心魔蝌蚪罐头")
                yield event.plain_result("\n".join(result_msg))
                return
            else:
                result_msg = [
                    "⚔️ 【牛牛对决结果】 ⚔️",
                    f"⚠️ {nickname} 使用夺心魔蝌蚪罐头，但是罐头好像坏掉了...",
                    f"🗡️ {nickname}: {self.format_length(user_data['length'])}",
                    f"🛡️ {target_data['nickname']}: {self.format_length(target_data['length'])}"
                ]
                self.shop.consume_item(group_id, user_id, "夺心魔蝌蚪罐头")
                yield event.plain_result("\n".join(result_msg))
                return

        u_len = user_data['length']
        t_len = target_data['length']
        u_hardness = user_data.get('hardness', 1)
        t_hardness = target_data.get('hardness', 1)
        base_win = 0.5
        length_factor = (u_len - t_len) / max(u_len, t_len) * 0.2 if max(u_len, t_len) > 0 else 0
        hardness_factor = (u_hardness - t_hardness) * 0.05
        win_prob = min(max(base_win + length_factor + hardness_factor, 0.2), 0.8)

        old_u_len = u_len
        old_t_len = t_len

        if random.random() < win_prob:
            gain = random.randint(0, 3)
            loss = random.randint(1, 2)
            updated_user = {'length': user_data['length'] + gain}
            updated_target = {'length': max(1, target_data['length'] - loss)}
            self.update_user_data(group_id, user_id, updated_user)
            self.update_user_data(group_id, target_id, updated_target)
            text = random.choice(self.niuniu_texts['compare']['win']).format(
                winner=nickname, loser=target_data['nickname'], gain=gain
            )
            total_gain = gain
            if (self.shop.get_user_items(group_id, user_id).get("淬火爪刀", 0) > 0 
                and abs(u_len - t_len) > 10 and u_len < t_len):
                extra_loot = int(target_data['length'] * 0.1)
                updated_user = {'length': user_data['length'] + gain + extra_loot}
                self.update_user_data(group_id, user_id, updated_user)
                total_gain += extra_loot
                text += f"\n🔥 淬火爪刀触发！额外掠夺 {extra_loot}cm！"
                self.shop.consume_item(group_id, user_id, "淬火爪刀")
            if abs(u_len - t_len) >= 20 and u_hardness < t_hardness:
                extra_gain = random.randint(0, 5)
                updated_user = {'length': user_data['length'] + gain + extra_gain}
                self.update_user_data(group_id, user_id, updated_user)
                total_gain += extra_gain
                text += f"\n🎁 由于极大劣势获胜，额外增加 {extra_gain}cm！"
            if abs(u_len - t_len) > 10 and u_len < t_len:
                stolen_length = int(target_data['length'] * 0.2)
                updated_user = {'length': user_data['length'] + gain + stolen_length}
                updated_target = {'length': max(1, target_data['length'] - loss - stolen_length)}
                self.update_user_data(group_id, user_id, updated_user)
                self.update_user_data(group_id, target_id, updated_target)
                total_gain += stolen_length
                text += f"\n🎉 {nickname} 掠夺了 {stolen_length}cm！"
            if total_gain == 0:
                text += f"\n{self.niuniu_texts['compare']['user_no_increase'].format(nickname=nickname)}"
        else:
            gain = random.randint(0, 3)
            loss = random.randint(1, 2)
            updated_target = {'length': target_data['length'] + gain}
            if self.shop.consume_item(group_id, user_id, "余震"):
                self.update_user_data(group_id, target_id, updated_target)
            else:
                updated_user = {'length': max(1, user_data['length'] - loss)}
                self.update_user_data(group_id, user_id, updated_user)
                self.update_user_data(group_id, target_id, updated_target)
            text = random.choice(self.niuniu_texts['compare']['lose']).format(
                loser=nickname, winner=target_data['nickname'], loss=loss
            )

        # 重新获取最新数据
        user_data = self.get_user_data(group_id, user_id)
        target_data = self.get_user_data(group_id, target_id)

        # 硬度衰减
        if random.random() < 0.3:
            updated_user = {'hardness': max(1, user_data.get('hardness', 1) - 1)}
            self.update_user_data(group_id, user_id, updated_user)
        if random.random() < 0.3:
            updated_target = {'hardness': max(1, target_data.get('hardness', 1) - 1)}
            self.update_user_data(group_id, target_id, updated_target)

        user_data = self.get_user_data(group_id, user_id)
        target_data = self.get_user_data(group_id, target_id)

        result_msg = [
            "⚔️ 【牛牛对决结果】 ⚔️",
            f"🗡️ {nickname}: {self.format_length(old_u_len)} → {self.format_length(user_data['length'])}",
            f"🛡️ {target_data['nickname']}: {self.format_length(old_t_len)} → {self.format_length(target_data['length'])}",
            f"📢 {text}"
        ]

        # 特殊事件
        special_event_triggered = False
        if abs(u_len - t_len) <= 5 and random.random() < 0.075:
            result_msg.append("💥 双方势均力敌！")
            special_event_triggered = True
        if not special_event_triggered and (user_data.get('hardness', 1) <= 2 or target_data.get('hardness', 1) <= 2) and random.random() < 0.05:
            original_user_len = user_data['length']
            original_target_len = target_data['length']
            updated_user = {'length': max(1, original_user_len // 2)}
            updated_target = {'length': max(1, original_target_len // 2)}
            self.update_user_data(group_id, user_id, updated_user)
            self.update_user_data(group_id, target_id, updated_target)
            if self.shop.get_user_items(group_id, user_id).get("妙脆角", 0) > 0:
                updated_user = {'length': original_user_len}
                self.update_user_data(group_id, user_id, updated_user)
                result_msg.append(f"🛡️ {nickname} 的妙脆角生效，防止了长度减半！")
                self.shop.consume_item(group_id, user_id, "妙脆角")
            if self.shop.get_user_items(group_id, target_id).get("妙脆角", 0) > 0:
                updated_target = {'length': original_target_len}
                self.update_user_data(group_id, target_id, updated_target)
                result_msg.append(f"🛡️ {target_data['nickname']} 的妙脆角生效，防止了长度减半！")
                self.shop.consume_item(group_id, target_id, "妙脆角")
            result_msg.append("双方牛牛因过于柔软发生缠绕！")
            special_event_triggered = True
        if not special_event_triggered and abs(u_len - t_len) < 10 and random.random() < 0.025:
            original_user_len = user_data['length']
            original_target_len = target_data['length']
            updated_user = {'length': max(1, original_user_len // 2)}
            updated_target = {'length': max(1, original_target_len // 2)}
            self.update_user_data(group_id, user_id, updated_user)
            self.update_user_data(group_id, target_id, updated_target)
            if self.shop.get_user_items(group_id, user_id).get("妙脆角", 0) > 0:
                updated_user = {'length': original_user_len}
                self.update_user_data(group_id, user_id, updated_user)
                result_msg.append(f"🛡️ {nickname} 的妙脆角生效，防止了长度减半！")
                self.shop.consume_item(group_id, user_id, "妙脆角")
            if self.shop.get_user_items(group_id, target_id).get("妙脆角", 0) > 0:
                updated_target = {'length': original_target_len}
                self.update_user_data(group_id, target_id, updated_target)
                result_msg.append(f"🛡️ {target_data['nickname']} 的妙脆角生效，防止了长度减半！")
                self.shop.consume_item(group_id, target_id, "妙脆角")
            result_msg.append(self.niuniu_texts['compare']['double_loss'].format(nickname1=nickname, nickname2=target_data['nickname']))
            special_event_triggered = True

        yield event.plain_result("\n".join(result_msg))

    async def _show_status(self, event):
        group_id = str(event.message_obj.group_id)
        user_id = str(event.get_sender_id())
        nickname = event.get_sender_name()
        user_data = self.get_user_data(group_id, user_id)
        if not user_data:
            yield event.plain_result(self.niuniu_texts['my_niuniu']['not_registered'].format(nickname=nickname))
            return
        length = user_data['length']
        length_str = self.format_length(length)
        if length < 12:
            evaluation = random.choice(self.niuniu_texts['my_niuniu']['evaluation']['short'])
        elif length < 25:
            evaluation = random.choice(self.niuniu_texts['my_niuniu']['evaluation']['medium'])
        elif length < 50:
            evaluation = random.choice(self.niuniu_texts['my_niuniu']['evaluation']['long'])
        elif length < 100:
            evaluation = random.choice(self.niuniu_texts['my_niuniu']['evaluation']['very_long'])
        elif length < 200:
            evaluation = random.choice(self.niuniu_texts['my_niuniu']['evaluation']['super_long'])
        else:
            evaluation = random.choice(self.niuniu_texts['my_niuniu']['evaluation']['ultra_long'])
        hardness = user_data.get('hardness', 1)
        text = self.niuniu_texts['my_niuniu']['info'].format(
            nickname=nickname, length=length_str, hardness=hardness, evaluation=evaluation
        )
        yield event.plain_result(text)

    async def _show_ranking(self, event):
        group_id = str(event.message_obj.group_id)
        data = self._load_niuniu_lengths()
        group_data = data.get(group_id, {'plugin_enabled': False})
        valid_users = [
            (uid, u_data) for uid, u_data in group_data.items()
            if isinstance(u_data, dict) and 'length' in u_data
        ]
        if not valid_users:
            yield event.plain_result(self.niuniu_texts['ranking']['no_data'])
            return
        sorted_users = sorted(valid_users, key=lambda x: x[1]['length'], reverse=True)[:10]
        ranking = [self.niuniu_texts['ranking']['header']]
        for idx, (uid, u_data) in enumerate(sorted_users, 1):
            ranking.append(
                self.niuniu_texts['ranking']['item'].format(
                    rank=idx, name=u_data.get('nickname', uid), length=self.format_length(u_data['length'])
                )
            )
        yield event.plain_result("\n".join(ranking))

    async def _show_menu(self, event):
        yield event.plain_result(self.niuniu_texts['menu']['default'])

    # ========== 签到与金融方法 ==========
    def _get_wealth_info(self, user_data: dict) -> tuple:
        total = user_data.get('coins', 0.0) + user_data.get('bank', 0.0)
        for min_coin, name, rate in reversed(WEALTH_LEVELS):
            if total >= min_coin:
                return name, rate
        return "平民", 0.25

    def _calculate_dynamic_wealth_value(self, user_data: dict, user_id: str) -> float:
        total = user_data.get('coins', 0.0) + user_data.get('bank', 0.0)
        base_value = WEALTH_BASE_VALUES["平民"]
        for min_coin, name, _ in reversed(WEALTH_LEVELS):
            if total >= min_coin:
                base_value = WEALTH_BASE_VALUES[name]
                break
        contract_level = self.purchase_data.get(str(user_id), 0)
        price_bonus = self.config.get('contract_level_price_bonus', 0.15)
        return base_value * (1 + contract_level * price_bonus)

    def _get_total_contractor_rate(self, group_id: str, contractor_ids: list) -> float:
        total_rate = 0.0
        rate_bonus = self.config.get('contract_level_rate_bonus', 0.075)
        for contractor_id in contractor_ids:
            contractor_data = self.get_user_data(group_id, contractor_id)
            if contractor_data:
                _, base_rate = self._get_wealth_info(contractor_data)
                contract_level = self.purchase_data.get(contractor_id, 0)
                total_rate += base_rate + (contract_level * rate_bonus)
        return total_rate

    async def _get_user_name_from_platform(self, event: AstrMessageEvent, target_id: str) -> str:
        if event.get_platform_name() == "aiocqhttp":
            try:
                from astrbot.core.platform.sources.aiocqhttp.aiocqhttp_message_event import AiocqhttpMessageEvent
                if isinstance(event, AiocqhttpMessageEvent):
                    client = event.bot
                    resp = await client.api.call_action(
                        "get_group_member_info",
                        group_id=event.message_obj.group_id,
                        user_id=int(target_id),
                        no_cache=True,
                    )
                    return resp.get("card") or resp.get("nickname", f"用户{target_id[-4:]}")
            except Exception:
                pass
        return f"用户{target_id[-4:]}"

    async def sign_in(self, event: AstrMessageEvent):
        group_id = str(event.message_obj.group_id)
        user_id = str(event.get_sender_id())
        user_data = self.get_user_data(group_id, user_id)
        if not user_data:
            yield event.plain_result("请先注册牛牛")
            return
        now = datetime.now(SHANGHAI_TZ)
        today = now.date()
        if user_data.get('last_sign'):
            try:
                last_sign_dt = datetime.fromisoformat(user_data['last_sign'])
                last_sign_aware = SHANGHAI_TZ.localize(last_sign_dt)
                if last_sign_aware.date() == today:
                    yield event.plain_result("你今天已经签到过了，明天再来吧。")
                    return
                if (today - last_sign_aware.date()).days == 1:
                    user_data['consecutive'] = user_data.get('consecutive', 0) + 1
                else:
                    user_data['consecutive'] = 1
            except:
                user_data['consecutive'] = 1
        else:
            user_data['consecutive'] = 1

        interest_earned = await self._calculate_and_apply_interest(user_data)
        interest_msg = f"\n💰 银行利息：+{interest_earned:.2f} 金币" if interest_earned > 0 else ""

        _, user_base_rate = self._get_wealth_info(user_data)
        contractor_dynamic_rates = self._get_total_contractor_rate(group_id, user_data.get('contractors', []))
        consecutive_bonus = 10 * (user_data.get('consecutive', 0) - 1)
        earned = BASE_INCOME * (1 + user_base_rate) * (1 + contractor_dynamic_rates) + consecutive_bonus
        original_earned = earned
        is_penalized = False
        if user_data.get('contracted_by'):
            income_rate = self.config.get("employed_income_rate", 0.7)
            earned *= income_rate
            is_penalized = True

        user_data['coins'] = user_data.get('coins', 0.0) + earned
        user_data['last_sign'] = now.replace(tzinfo=None).isoformat()

        # 雇员收益累积（雇主）
        employer_id = user_data.get('contracted_by')
        if employer_id:
            employer_data = self.get_user_data(group_id, employer_id)
            if employer_data:
                earnings = employer_data.get('employee_earnings', {})
                earnings[user_id] = earnings.get(user_id, 0.0) + earned
                employer_data['employee_earnings'] = earnings
                self.update_user_data(group_id, employer_id, employer_data)

        self.update_user_data(group_id, user_id, user_data)

        msg_lines = [
            f"✅ 签到成功！",
            f"📅 连续签到：{user_data['consecutive']} 天",
            f"💵 基础收益：{earned:.2f} 金币",
        ]
        if is_penalized:
            msg_lines.append(f"⚠️ 受雇惩罚：原收益 {original_earned:.2f} → {earned:.2f}")
        msg_lines.append(f"💰 当前现金：{user_data['coins']:.2f} 金币")
        msg_lines.append(f"🏦 银行存款：{user_data['bank']:.2f} 金币")
        if interest_msg:
            msg_lines.append(interest_msg.strip())
        yield event.plain_result("\n".join(msg_lines))

    async def deposit(self, event: AstrMessageEvent, amount_str: str):
        try:
            amount = float(amount_str)
            if amount <= 0:
                yield event.plain_result("存款金额必须大于0。")
                return
        except ValueError:
            yield event.plain_result("金额格式不正确，请使用：存款 <数字>")
            return
        group_id = str(event.message_obj.group_id)
        user_id = str(event.get_sender_id())
        user_data = self.get_user_data(group_id, user_id)
        if not user_data:
            yield event.plain_result("请先注册牛牛")
            return
        # 存款前先结算利息
        await self._calculate_and_apply_interest(user_data)
        if amount > user_data.get('coins', 0.0):
            yield event.plain_result(f"现金不足，当前现金：{user_data.get('coins', 0.0):.2f}")
            return
        user_data['coins'] -= amount
        user_data['bank'] = user_data.get('bank', 0.0) + amount
        if user_data.get('last_interest_time') is None:
            user_data['last_interest_time'] = datetime.now(SHANGHAI_TZ).isoformat()
        self.update_user_data(group_id, user_id, user_data)
        yield event.plain_result(f"成功存入 {amount:.2f} 金币到银行。当前存款：{user_data['bank']:.2f}")

    async def withdraw(self, event: AstrMessageEvent, amount_str: str):
        try:
            amount = float(amount_str)
            if amount <= 0:
                yield event.plain_result("取款金额必须大于0。")
                return
        except ValueError:
            yield event.plain_result("金额格式不正确，请使用：取款 <数字>")
            return
        group_id = str(event.message_obj.group_id)
        user_id = str(event.get_sender_id())
        user_data = self.get_user_data(group_id, user_id)
        if not user_data:
            yield event.plain_result("请先注册牛牛")
            return
        if amount > user_data.get('bank', 0.0):
            yield event.plain_result(f"银行存款不足，当前存款：{user_data.get('bank', 0.0):.2f}")
            return
        user_data['bank'] -= amount
        user_data['coins'] = user_data.get('coins', 0.0) + amount
        self.update_user_data(group_id, user_id, user_data)
        yield event.plain_result(f"成功取出 {amount:.2f} 金币。当前现金：{user_data['coins']:.2f}")

    async def bank_info(self, event: AstrMessageEvent):
        group_id = str(event.message_obj.group_id)
        user_id = str(event.get_sender_id())
        user_data = self.get_user_data(group_id, user_id)
        if not user_data:
            yield event.plain_result("请先注册牛牛")
            return
        bank_amount = user_data.get('bank', 0.0)
        per_minute = bank_amount * INTEREST_RATE_PER_MINUTE
        per_hour = per_minute * 60
        per_day = per_hour * 24
        last_time_str = user_data.get('last_interest_time')
        if last_time_str:
            last_time = datetime.fromisoformat(last_time_str)
            if last_time.tzinfo is None:
                last_time = SHANGHAI_TZ.localize(last_time)
            now = datetime.now(SHANGHAI_TZ)
            elapsed_minutes = (now - last_time).total_seconds() / 60
            pending_interest = bank_amount * INTEREST_RATE_PER_MINUTE * elapsed_minutes
        else:
            pending_interest = 0.0
        msg = (
            f"🏦 银行账户信息\n"
            f"存款余额：{bank_amount:.2f} 金币\n"
            f"利率：每分钟 {INTEREST_RATE_PER_MINUTE*100:.2f}%（即每金币每分钟生息 0.001）\n"
            f"每分钟收益：{per_minute:.4f} 金币\n"
            f"每小时收益：{per_hour:.4f} 金币\n"
            f"每日收益：{per_day:.4f} 金币\n"
            f"待领取利息：{pending_interest:.4f} 金币"
        )
        yield event.plain_result(msg)

    async def _calculate_and_apply_interest(self, user_data: dict) -> float:
        bank_amount = user_data.get('bank', 0.0)
        if bank_amount <= 0:
            return 0.0
        last_time_str = user_data.get('last_interest_time')
        now = datetime.now(SHANGHAI_TZ)
        if last_time_str:
            try:
                last_time = datetime.fromisoformat(last_time_str)
                if last_time.tzinfo is None:
                    last_time = SHANGHAI_TZ.localize(last_time)
            except:
                last_time = now
        else:
            last_time = now
        elapsed_minutes = (now - last_time).total_seconds() / 60
        if elapsed_minutes <= 0:
            return 0.0
        interest = bank_amount * INTEREST_RATE_PER_MINUTE * elapsed_minutes
        if interest > 0:
            user_data['coins'] = user_data.get('coins', 0.0) + interest
            user_data['last_interest_time'] = now.isoformat()
        return interest

    async def claim_interest(self, event: AstrMessageEvent):
        group_id = str(event.message_obj.group_id)
        user_id = str(event.get_sender_id())
        user_data = self.get_user_data(group_id, user_id)
        if not user_data:
            yield event.plain_result("请先注册牛牛")
            return
        if user_data.get('bank', 0.0) <= 0:
            yield event.plain_result("银行存款为0，无法产生利息。")
            return
        interest = await self._calculate_and_apply_interest(user_data)
        self.update_user_data(group_id, user_id, user_data)
        if interest > 0:
            yield event.plain_result(f"✅ 成功领取利息 {interest:.4f} 金币！当前现金：{user_data['coins']:.2f}")
        else:
            yield event.plain_result("暂无待领取的利息。")

    async def transfer(self, event: AstrMessageEvent, target_id: str, amount_str: str):
        try:
            amount = float(amount_str)
            if amount <= 0:
                yield event.plain_result("转账金额必须大于0。")
                return
        except ValueError:
            yield event.plain_result("金额格式不正确，请使用：转账 @目标 金额")
            return
        group_id = str(event.message_obj.group_id)
        user_id = str(event.get_sender_id())
        if user_id == target_id:
            yield event.plain_result("不能转账给自己。")
            return
        sender_data = self.get_user_data(group_id, user_id)
        receiver_data = self.get_user_data(group_id, target_id)
        if not sender_data:
            yield event.plain_result("请先注册牛牛")
            return
        if not receiver_data:
            yield event.plain_result("对方尚未注册牛牛")
            return
        if sender_data.get('coins', 0.0) < amount:
            yield event.plain_result(f"现金不足，当前现金：{sender_data.get('coins', 0.0):.2f}")
            return
        sender_data['coins'] -= amount
        receiver_data['coins'] = receiver_data.get('coins', 0.0) + amount
        self.update_user_data(group_id, user_id, sender_data)
        self.update_user_data(group_id, target_id, receiver_data)
        target_name = await self._get_user_name_from_platform(event, target_id)
        yield event.plain_result(f"成功转账 {amount:.2f} 金币给 {target_name}。")

    # ========== 雇佣相关 ==========
    async def purchase_contractor(self, event: AstrMessageEvent):
        target_id = self.parse_at_target(event)
        if not target_id:
            yield event.plain_result("请使用@指定要购买的对象。")
            return
        group_id = str(event.message_obj.group_id)
        user_id = str(event.get_sender_id())
        if user_id == target_id:
            yield event.plain_result("您不能购买自己。")
            return
        employer_data = self.get_user_data(group_id, user_id)
        target_data = self.get_user_data(group_id, target_id)
        if not employer_data or not target_data:
            yield event.plain_result("双方都需要注册牛牛")
            return
        if len(employer_data.get('contractors', [])) >= 3:
            yield event.plain_result("已达到最大雇佣数量（3人）。")
            return
        base_cost = self._calculate_dynamic_wealth_value(target_data, target_id)
        total_cost = base_cost
        original_owner_id = target_data.get('contracted_by')
        if original_owner_id:
            if original_owner_id == user_id:
                yield event.plain_result("该用户已经是您的雇员了。")
                return
            takeover_rate = self.config.get("takeover_fee_rate", 0.1)
            extra_cost = base_cost * takeover_rate
            total_cost += extra_cost
            compensation = total_cost
            if employer_data.get('coins', 0.0) < total_cost:
                yield event.plain_result(f"现金不足，恶意收购需要支付 {total_cost:.1f} 金币。")
                return
            original_owner_data = self.get_user_data(group_id, original_owner_id)
            if original_owner_data and target_id in original_owner_data.get('contractors', []):
                original_owner_data['contractors'].remove(target_id)
                original_owner_data['coins'] = original_owner_data.get('coins', 0.0) + compensation
                self.update_user_data(group_id, original_owner_id, original_owner_data)
            employer_data['coins'] = employer_data.get('coins', 0.0) - total_cost
            employer_data.setdefault('contractors', []).append(target_id)
            target_data['contracted_by'] = user_id
            self.purchase_data[target_id] = self.purchase_data.get(target_id, 0) + 1
            self.update_user_data(group_id, user_id, employer_data)
            self.update_user_data(group_id, target_id, target_data)
            await self._save_purchase_data()
            target_name = await self._get_user_name_from_platform(event, target_id)
            original_owner_name = await self._get_user_name_from_platform(event, original_owner_id)
            yield event.plain_result(
                f"恶意收购成功！您花费 {total_cost:.1f} 金币从 {original_owner_name} 手中抢走了 {target_name}。"
                f"原雇主获得了全部转让费 {compensation:.1f} 金币。"
            )
            return
        if employer_data.get('coins', 0.0) < total_cost:
            yield event.plain_result(f"现金不足，雇佣需要支付目标身价：{total_cost:.1f}金币。")
            return
        employer_data['coins'] = employer_data.get('coins', 0.0) - total_cost
        employer_data.setdefault('contractors', []).append(target_id)
        target_data['contracted_by'] = user_id
        self.purchase_data[target_id] = self.purchase_data.get(target_id, 0) + 1
        self.update_user_data(group_id, user_id, employer_data)
        self.update_user_data(group_id, target_id, target_data)
        await self._save_purchase_data()
        target_name = await self._get_user_name_from_platform(event, target_id)
        yield event.plain_result(f"成功雇佣 {target_name}，消耗{total_cost:.1f}金币。")

    async def sell_contractor(self, event: AstrMessageEvent):
        target_id = self.parse_at_target(event)
        if not target_id:
            yield event.plain_result("请使用@指定要出售的对象。")
            return
        group_id = str(event.message_obj.group_id)
        user_id = str(event.get_sender_id())
        employer_data = self.get_user_data(group_id, user_id)
        target_data = self.get_user_data(group_id, target_id)
        if not employer_data or not target_data:
            yield event.plain_result("双方都需要注册牛牛")
            return
        if target_id not in employer_data.get('contractors', []):
            yield event.plain_result("该用户不在你的雇员列表中。")
            return
        sell_rate = self.config.get("sell_return_rate", 0.8)
        sell_price = self._calculate_dynamic_wealth_value(target_data, target_id) * sell_rate
        employer_data['coins'] = employer_data.get('coins', 0.0) + sell_price
        employer_data['contractors'].remove(target_id)
        target_data['contracted_by'] = None
        self.update_user_data(group_id, user_id, employer_data)
        self.update_user_data(group_id, target_id, target_data)
        target_name = await self._get_user_name_from_platform(event, target_id)
        yield event.plain_result(f"成功解雇 {target_name}，获得补偿金{sell_price:.1f}金币。")

    async def terminate_contract(self, event: AstrMessageEvent):
        group_id = str(event.message_obj.group_id)
        user_id = str(event.get_sender_id())
        user_data = self.get_user_data(group_id, user_id)
        if not user_data:
            yield event.plain_result("请先注册牛牛")
            return
        if not user_data.get('contracted_by'):
            yield event.plain_result("您是自由身，无需赎身。")
            return
        cost = self._calculate_dynamic_wealth_value(user_data, user_id)
        if user_data.get('coins', 0.0) < cost:
            yield event.plain_result(f"金币不足，需要支付赎身费用：{cost:.1f}金币。")
            return
        employer_id = user_data['contracted_by']
        employer_data = self.get_user_data(group_id, employer_id)
        user_data['coins'] -= cost
        if employer_data and user_id in employer_data.get('contractors', []):
            employer_data['contractors'].remove(user_id)
            redeem_rate = self.config.get("redeem_return_rate", 0.5)
            compensation = cost * redeem_rate
            employer_data['coins'] = employer_data.get('coins', 0.0) + compensation
            self.update_user_data(group_id, employer_id, employer_data)
        user_data['contracted_by'] = None
        self.update_user_data(group_id, user_id, user_data)
        employer_name = await self._get_user_name_from_platform(event, employer_id) if employer_id else "未知雇主"
        yield event.plain_result(f"赎身成功，消耗{cost:.1f}金币，重获自由！原雇主 {employer_name} 获得了补偿。")

    # ========== 新增雇员管理 ==========
    async def show_employees(self, event: AstrMessageEvent):
        group_id = str(event.message_obj.group_id)
        user_id = str(event.get_sender_id())
        user_data = self.get_user_data(group_id, user_id)
        if not user_data:
            yield event.plain_result("请先注册牛牛")
            return
        contractors = user_data.get('contractors', [])
        if not contractors:
            yield event.plain_result("你还没有雇佣任何人。")
            return
        # 先更新雇员收益（基于时间累积）
        await self._update_employee_earnings_by_time(group_id, user_id, user_data)
        earnings = user_data.get('employee_earnings', {})
        lines = ["👥 你的雇员列表："]
        for cid in contractors:
            name = await self._get_user_name_from_platform(event, cid)
            earned = earnings.get(cid, 0.0)
            lines.append(f"• {name} - 累计收益：{earned:.2f} 金币")
        yield event.plain_result("\n".join(lines))

    async def _update_employee_earnings_by_time(self, group_id: str, employer_id: str, employer_data: dict):
        """根据时间累积雇员收益（每分钟身价*0.01）"""
        last_time_str = employer_data.get('employee_earnings_last_time')
        now = datetime.now(SHANGHAI_TZ)
        if not last_time_str:
            employer_data['employee_earnings_last_time'] = now.isoformat()
            return
        try:
            last_time = datetime.fromisoformat(last_time_str)
            if last_time.tzinfo is None:
                last_time = SHANGHAI_TZ.localize(last_time)
        except:
            employer_data['employee_earnings_last_time'] = now.isoformat()
            return
        elapsed_minutes = (now - last_time).total_seconds() / 60
        if elapsed_minutes <= 0:
            return
        contractors = employer_data.get('contractors', [])
        earnings = employer_data.get('employee_earnings', {})
        for cid in contractors:
            cdata = self.get_user_data(group_id, cid)
            if cdata:
                worth = self._calculate_dynamic_wealth_value(cdata, cid)
                add = worth * EMPLOYEE_EARNINGS_RATE * elapsed_minutes
                earnings[cid] = earnings.get(cid, 0.0) + add
        employer_data['employee_earnings'] = earnings
        employer_data['employee_earnings_last_time'] = now.isoformat()
        self.update_user_data(group_id, employer_id, employer_data)

    async def claim_employee_earnings(self, event: AstrMessageEvent):
        group_id = str(event.message_obj.group_id)
        user_id = str(event.get_sender_id())
        user_data = self.get_user_data(group_id, user_id)
        if not user_data:
            yield event.plain_result("请先注册牛牛")
            return
        await self._update_employee_earnings_by_time(group_id, user_id, user_data)
        earnings = user_data.get('employee_earnings', {})
        if not earnings:
            yield event.plain_result("暂无雇员收益可领取。")
            return
        total = sum(earnings.values())
        if total <= 0:
            yield event.plain_result("暂无雇员收益可领取。")
            return
        user_data['coins'] = user_data.get('coins', 0.0) + total
        user_data['employee_earnings'] = {}
        self.update_user_data(group_id, user_id, user_data)
        yield event.plain_result(f"成功领取雇员收益 {total:.2f} 金币，当前现金：{user_data['coins']:.2f}")

    # ========== 牛牛市场功能 ==========
    async def sell_length_market(self, event: AstrMessageEvent, msg: str):
        parts = msg.split()
        if len(parts) < 3:
            yield event.plain_result("格式：出售牛牛 <长度> <价格>")
            return
        try:
            length_to_sell = int(parts[1])
            price = float(parts[2])
        except ValueError:
            yield event.plain_result("长度和价格必须是数字")
            return
        if length_to_sell <= 0 or price <= 0:
            yield event.plain_result("长度和价格必须大于0")
            return
        group_id = str(event.message_obj.group_id)
        user_id = str(event.get_sender_id())
        user_data = self.get_user_data(group_id, user_id)
        if not user_data:
            yield event.plain_result("请先注册牛牛")
            return
        if user_data['length'] < length_to_sell:
            yield event.plain_result(f"你的牛牛长度不足，当前长度：{self.format_length(user_data['length'])}")
            return
        # 扣除长度，创建挂单
        user_data['length'] -= length_to_sell
        listing = {
            'id': len(self.market_listings) + 1,
            'seller_id': user_id,
            'seller_name': user_data['nickname'],
            'length': length_to_sell,
            'price': price,
            'group_id': group_id
        }
        self.market_listings.append(listing)
        self.update_user_data(group_id, user_id, user_data)
        await self._save_market_data()
        yield event.plain_result(f"成功挂单！编号：{listing['id']}，出售 {self.format_length(length_to_sell)}，价格 {price:.2f} 金币")

    async def show_market(self, event: AstrMessageEvent):
        if not self.market_listings:
            yield event.plain_result("当前市场没有挂单。")
            return
        lines = ["📋 牛牛市场挂单列表："]
        for l in self.market_listings:
            lines.append(f"{l['id']}. {l['seller_name']} 出售 {self.format_length(l['length'])} 价格 {l['price']:.2f} 金币")
        yield event.plain_result("\n".join(lines))

    async def buy_from_market(self, event: AstrMessageEvent, listing_id_str: str):
        try:
            listing_id = int(listing_id_str)
        except ValueError:
            yield event.plain_result("编号必须是数字")
            return
        group_id = str(event.message_obj.group_id)
        user_id = str(event.get_sender_id())
        buyer_data = self.get_user_data(group_id, user_id)
        if not buyer_data:
            yield event.plain_result("请先注册牛牛")
            return
        # 查找挂单
        listing = None
        for l in self.market_listings:
            if l['id'] == listing_id and l['group_id'] == group_id:
                listing = l
                break
        if not listing:
            yield event.plain_result("找不到该挂单")
            return
        if buyer_data.get('coins', 0.0) < listing['price']:
            yield event.plain_result(f"金币不足，需要 {listing['price']:.2f} 金币")
            return
        # 扣钱、加长度
        buyer_data['coins'] -= listing['price']
        buyer_data['length'] += listing['length']
        # 卖家收钱
        seller_data = self.get_user_data(group_id, listing['seller_id'])
        if seller_data:
            seller_data['coins'] = seller_data.get('coins', 0.0) + listing['price']
            self.update_user_data(group_id, listing['seller_id'], seller_data)
        self.update_user_data(group_id, user_id, buyer_data)
        self.market_listings.remove(listing)
        await self._save_market_data()
        yield event.plain_result(f"购买成功！获得 {self.format_length(listing['length'])}，花费 {listing['price']:.2f} 金币")

    # ========== 撅指令 ==========
    async def jue(self, event: AstrMessageEvent, target_id: str):
        group_id = str(event.message_obj.group_id)
        user_id = str(event.get_sender_id())
        if user_id == target_id:
            yield event.plain_result("不能撅自己！")
            return
        user_data = self.get_user_data(group_id, user_id)
        target_data = self.get_user_data(group_id, target_id)
        if not user_data or not target_data:
            yield event.plain_result("双方都需要注册牛牛")
            return
        cost = 100.0
        if user_data.get('coins', 0.0) < cost:
            yield event.plain_result(f"现金不足，需要 {cost:.0f} 金币")
            return
        # 扣除金币
        user_data['coins'] -= cost
        target_data['coins'] = target_data.get('coins', 0.0) + cost
        # 夺取长度
        steal = random.randint(1, 20)
        actual_steal = min(steal, target_data['length'] - 1)
        if actual_steal > 0:
            target_data['length'] -= actual_steal
            user_data['length'] += actual_steal
        self.update_user_data(group_id, user_id, user_data)
        self.update_user_data(group_id, target_id, target_data)
        target_name = await self._get_user_name_from_platform(event, target_id)
        yield event.plain_result(
            f"😈 {user_data['nickname']} 撅了 {target_name}！\n"
            f"支付 {cost:.0f} 金币，夺取长度 {actual_steal}cm！"
        )

    # ========== 财富排行榜 ==========
    async def wealth_leaderboard(self, event: AstrMessageEvent):
        group_id = str(event.message_obj.group_id)
        data = self._load_niuniu_lengths()
        group_data = data.get(group_id, {})
        if not group_data:
            yield event.plain_result("本群暂无数据。")
            return
        all_users_wealth = []
        for user_id, u_data in group_data.items():
            if isinstance(u_data, dict) and 'coins' in u_data:
                total = u_data.get('coins', 0.0) + u_data.get('bank', 0.0)
                all_users_wealth.append((user_id, total))
        sorted_users = sorted(all_users_wealth, key=lambda x: x[1], reverse=True)[:10]
        if not sorted_users:
            yield event.plain_result("本群暂无财富数据。")
            return
        user_ids = [u[0] for u in sorted_users]
        names = await asyncio.gather(*[self._get_user_name_from_platform(event, uid) for uid in user_ids])
        leaderboard_str = "💰 本群财富排行榜\n" + "-" * 20 + "\n"
        for rank, ((user_id, total), name) in enumerate(zip(sorted_users, names), 1):
            leaderboard_str += f"第{rank}名: {name} - {total:.2f} 金币\n"
        yield event.plain_result(leaderboard_str.strip())

    async def sign_query(self, event: AstrMessageEvent):
        group_id = str(event.message_obj.group_id)
        user_id = str(event.get_sender_id())
        user_data = self.get_user_data(group_id, user_id)
        if not user_data:
            yield event.plain_result("请先注册牛牛")
            return
        wealth_level, _ = self._get_wealth_info(user_data)
        contractors = user_data.get('contractors', [])
        contractor_names = []
        for cid in contractors:
            name = await self._get_user_name_from_platform(event, cid)
            contractor_names.append(name)
        employer = user_data.get('contracted_by')
        employer_name = await self._get_user_name_from_platform(event, employer) if employer else "无"
        msg = (
            f"📋 {user_data['nickname']} 的签到信息\n"
            f"💰 现金：{user_data.get('coins', 0.0):.2f} 金币\n"
            f"🏦 存款：{user_data.get('bank', 0.0):.2f} 金币\n"
            f"📊 财富等级：{wealth_level}\n"
            f"📅 连续签到：{user_data.get('consecutive', 0)} 天\n"
            f"👥 雇佣者：{employer_name}\n"
            f"👤 雇员：{', '.join(contractor_names) if contractor_names else '无'}\n"
            f"🕒 上次签到：{user_data.get('last_sign', '从未')}"
        )
        yield event.plain_result(msg)

    async def terminate(self):
        pass
