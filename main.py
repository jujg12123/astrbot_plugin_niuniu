import random
import yaml
import os
import re
import time
import json
import sys
from astrbot.api.all import *
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from niuniu_shop import NiuniuShop
from niuniu_games import NiuniuGames

# 常量定义
PLUGIN_DIR = os.path.join('data', 'plugins', 'astrbot_plugin_niuniu')
os.makedirs(PLUGIN_DIR, exist_ok=True)
NIUNIU_LENGTHS_FILE = os.path.join('data', 'niuniu_lengths.yml')
NIUNIU_TEXTS_FILE = os.path.join(PLUGIN_DIR, 'niuniu_game_texts.yml')
LAST_ACTION_FILE = os.path.join(PLUGIN_DIR, 'last_actions.yml')

@register("niuniu_plugin", "长安某", "牛牛插件，包含注册牛牛、打胶、我的牛牛、比划比划、牛牛排行等功能", "4.7.2")
class NiuniuPlugin(Star):
    # 冷却时间常量（秒）
    COOLDOWN_10_MIN = 600    # 10分钟
    COOLDOWN_30_MIN = 1800   # 30分钟
    COMPARE_COOLDOWN = 600   # 比划冷却
    INVITE_LIMIT = 3         # 邀请次数限制

    def __init__(self, context: Context, config: dict = None):
        super().__init__(context)
        self.config = config or {}
        self.niuniu_texts = self._load_niuniu_texts()
        self.last_actions = self._load_last_actions()
        self.admins = self._load_admins()
        self.shop = NiuniuShop(self)
        self.games = NiuniuGames(self)
    
    # region 数据文件操作
    def _create_niuniu_lengths_file(self):
        try:
            with open(NIUNIU_LENGTHS_FILE, 'w', encoding='utf-8') as f:
                yaml.dump({}, f)
        except Exception as e:
            self.context.logger.error(f"创建文件失败: {str(e)}")

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
                for user_id in list(group_data.keys()):
                    user_data = group_data[user_id]
                    if isinstance(user_data, dict):
                        user_data.setdefault('coins', 0)
                        user_data.setdefault('items', {}) 
            return data
        except Exception as e:
            self.context.logger.error(f"加载数据失败: {str(e)}")
            return {}

    def _save_niuniu_lengths(self, data):
        try:
            with open(NIUNIU_LENGTHS_FILE, 'w', encoding='utf-8') as f:
                yaml.dump(data, f, allow_unicode=True)
        except Exception as e:
            self.context.logger.error(f"保存失败: {str(e)}")

    def _load_niuniu_texts(self):
        default_texts = {
            'register': {
                'success': "🧧 {nickname} 成功注册牛牛！\n📏 初始长度：{length}cm\n💪 硬度等级：{hardness}",
                'already_registered': "⚠️ {nickname} 你已经注册过牛牛啦！",
            },
            'dajiao': {
                'cooldown': [
                    "⏳ {nickname} 牛牛需要休息，{remaining}分钟后可再打胶",
                    "🛑 冷却中，{nickname} 请耐心等待 (＞﹏＜)"
                ],
                'increase': [
                    "🚀 {nickname} 打胶成功！长度增加 {change}cm！",
                    "🎉 {nickname} 的牛牛茁壮成长！+{change}cm"
                ],
                'decrease': [
                    "😱 {nickname} 用力过猛！长度减少 {change}cm！",
                    "⚠️ {nickname} 操作失误！-{change}cm"
                ],
                'decrease_30min': [
                    "😱 {nickname} 用力过猛！长度减少 {change}cm！",
                    "⚠️ {nickname} 操作失误！-{change}cm"
                ],
                'no_effect': [
                    "🌀 {nickname} 的牛牛毫无变化...",
                    "🔄 {nickname} 这次打胶没有效果"
                ],
                'not_registered': "❌ {nickname} 请先注册牛牛"
            },
            'my_niuniu': {
                'info': "📊 {nickname} 的牛牛状态\n📏 长度：{length}\n💪 硬度：{hardness}\n📝 评价：{evaluation}",
                'evaluation': {
                    'short': ["小巧玲珑", "精致可爱"],
                    'medium': ["中规中矩", "潜力无限"],
                    'long': ["威风凛凛", "傲视群雄"],
                    'very_long': ["擎天巨柱", "突破天际"],
                    'super_long': ["超级长", "无与伦比"],
                    'ultra_long': ["超越极限", "无人能敌"]
                },
                'not_registered': "❌ {nickname} 请先注册牛牛"
            },
            'compare': {
                'no_target': "❌ {nickname} 请指定比划对象",
                'target_not_registered': "❌ 对方尚未注册牛牛",
                'cooldown': "⏳ {nickname} 请等待{remaining}分钟后再比划",
                'self_compare': "❌ 不能和自己比划",
                'win': [
                    "🎉 {winner} 战胜了 {loser}！\n📈 增加 {gain}cm",
                    "🏆 {winner} 的牛牛更胜一筹！+{gain}cm"
                ],
                'lose': [
                    "😭 {loser} 败给 {winner}\n📉 减少 {loss}cm",
                    "💔 {loser} 的牛牛不敌对方！-{loss}cm"
                ],
                'draw': "🤝 双方势均力敌！",
                'double_loss': "😱 {nickname1} 和 {nickname2} 的牛牛因过于柔软发生缠绕，长度减半！",
                'hardness_win': "🎉 {nickname} 因硬度优势获胜！",
                'hardness_lose': "💔 {nickname} 因硬度劣势败北！",
                'user_no_increase': "😅 {nickname} 的牛牛没有任何增长。"
            },
            'ranking': {
                'header': "🏅 牛牛排行榜 TOP10：\n",
                'no_data': "📭 本群暂无牛牛数据",
                'item': "{rank}. {name} ➜ {length}"
            },
            'menu': {
                'default': """📜 牛牛菜单：
🔹 注册牛牛 - 初始化你的牛牛
🔹 打胶 - 提升牛牛长度
🔹 开冲 - 挂机赚金币
🔹 飞飞机 - 赚金币
🔹 我的牛牛 - 查看当前状态
🔹 比划比划 @目标 - 发起对决
🔹 牛牛排行 - 查看群排行榜
🔹 牛牛商城 - 查看可购买的物品
🔹 牛牛背包 - 查看现有道具和金币数量
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
        except Exception as e:
            self.context.logger.error(f"加载管理员列表失败: {str(e)}")
            return []

    def is_admin(self, user_id):
        return str(user_id) in self.admins
    # endregion

    # region 数据访问接口
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
            'nickname': '',
            'length': 0,
            'hardness': 1,
            'coins': 0,
            'items': {}
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

    def update_last_actions(self, data):
        self._save_last_actions(data)
    # endregion

    # region 工具方法
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
                    if isinstance(user_data, dict): 
                        nickname = user_data.get('nickname', '')
                        if re.search(re.escape(target_name), nickname, re.IGNORECASE):
                            return user_id
        return None
    # endregion

    # region 事件处理
    niuniu_commands = ["牛牛菜单", "牛牛开", "牛牛关", "注册牛牛", "打胶", "我的牛牛", "比划比划", "牛牛排行"]

    @event_message_type(EventMessageType.GROUP_MESSAGE)
    async def on_group_message(self, event: AstrMessageEvent):
        group_id = str(event.message_obj.group_id)
        group_data = self.get_group_data(group_id)

        msg = event.message_str.strip()
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

        if msg.startswith("开冲"):
            if is_rushing:
                yield event.plain_result("❌ 你已经在开冲了，无需重复操作")
                return
            async for result in self.games.start_rush(event):
                yield result
        elif msg.startswith("停止开冲"):
            if not is_rushing:
                yield event.plain_result("❌ 你当前并未在开冲，无需停止")
                return
            async for result in self.games.stop_rush(event):
                yield result
        elif msg.startswith("飞飞机"):
            if is_rushing:
                yield event.plain_result("❌ 牛牛快冲晕了，还做不了其他事情，要不先停止开冲？")
                return
            async for result in self.games.fly_plane(event):
                yield result
        else:
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
            "开冲", "停止开冲", "飞飞机"  
        ]
        
        if any(msg.startswith(cmd) for cmd in niuniu_commands):
            yield event.plain_result("不许一个人偷偷玩牛牛")
        else:
            return

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

        group_data = self.get_group_data(group_id)
        if not group_data.get('plugin_enabled', False):
            yield event.plain_result("❌ 插件未启用")
            return

        if self.get_user_data(group_id, user_id):
            text = self.niuniu_texts['register']['already_registered'].format(nickname=nickname)
            yield event.plain_result(text)
            return

        cfg = self.config.get('niuniu_config', {})
        user_data = {
            'nickname': nickname,
            'length': random.randint(cfg.get('min_length', 3), cfg.get('max_length', 10)),
            'hardness': 1,
            'coins': 0,
            'items': {}
        }
        self.update_user_data(group_id, user_id, user_data)

        text = self.niuniu_texts['register']['success'].format(
            nickname=nickname,
            length=user_data['length'],
            hardness=user_data['hardness']
        )
        yield event.plain_result(text)

    async def _dajiao(self, event: AstrMessageEvent):
        group_id = str(event.message_obj.group_id)
        user_id = str(event.get_sender_id())
        nickname = event.get_sender_name()

        group_data = self.get_group_data(group_id)
        if not group_data.get('plugin_enabled', False):
            yield event.plain_result("❌ 插件未启用")
            return

        user_data = self.get_user_data(group_id, user_id)
        if not user_data:
            text = self.niuniu_texts['dajiao']['not_registered'].format(nickname=nickname)
            yield event.plain_result(text)
            return

        user_items = self.shop.get_user_items(group_id, user_id)
        has_zhiming_rhythm = user_items.get("致命节奏", 0) > 0
        last_actions = self._load_last_actions()
        last_time = last_actions.setdefault(group_id, {}).get(user_id, {}).get('dajiao', 0)
        
        result_msg = []
        
        on_cooldown, remaining = self.check_cooldown(last_time, self.COOLDOWN_10_MIN)
        
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
                user_data['hardness'] = min(user_data['hardness'] + 1, 10)
            elif rand < 0.9:
                change = -random.randint(1, 2)
                template = random.choice(self.niuniu_texts['dajiao']['decrease_30min'])

        updated_data = {
            'length': max(1, user_data['length'] + change)
        }
        if 'hardness' in locals():
            updated_data['hardness'] = user_data['hardness']
        self.update_user_data(group_id, user_id, updated_data)

        last_actions = self._load_last_actions()
        last_actions.setdefault(group_id, {}).setdefault(user_id, {})['dajiao'] = current_time
        self.update_last_actions(last_actions)

        if change > 0:
            template = random.choice(self.niuniu_texts['dajiao']['increase'])
        elif change < 0:
            template = template  
        else:
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

        group_data = self.get_group_data(group_id)
        if not group_data.get('plugin_enabled', False):
            yield event.plain_result("❌ 插件未启用")
            return

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

        last_actions = self._load_last_actions()
        compare_records = last_actions.setdefault(group_id, {}).setdefault(user_id, {})
        last_compare = compare_records.get(target_id, 0)
        on_cooldown, remaining = self.check_cooldown(last_compare, self.COMPARE_COOLDOWN)
        if on_cooldown:
            mins = int(remaining // 60) + 1
            text = self.niuniu_texts['compare']['cooldown'].format(
                nickname=nickname,
                remaining=mins
            )
            yield event.plain_result(text)
            return

        last_compare_time = compare_records.get('last_time', 0)
        current_time = time.time()

        if current_time - last_compare_time > 600:
            compare_records['count'] = 0
            compare_records['last_time'] = current_time
            self.update_last_actions(last_actions)

        compare_count = compare_records.get('count', 0)

        if compare_count >= 3:
            yield event.plain_result("❌ 10分钟内只能比划三次")
            return

        compare_records[target_id] = current_time
        compare_records['count'] = compare_count + 1
        self.update_last_actions(last_actions)

        user_items = self.shop.get_user_items(group_id, user_id)
        if user_items.get("夺心魔蝌蚪罐头", 0) > 0:
            effect_chance = random.random()
            if effect_chance < 0.5:
                original_target_length = target_data['length']
                updated_user = {
                    'length': user_data['length'] + original_target_length
                }
                updated_target = {
                    'length': 1
                }
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
                updated_user = {
                    'length': 1
                }
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
        u_hardness = user_data['hardness']
        t_hardness = target_data['hardness']

        base_win = 0.5
        length_factor = (u_len - t_len) / max(u_len, t_len) * 0.2
        hardness_factor = (u_hardness - t_hardness) * 0.05
        win_prob = min(max(base_win + length_factor + hardness_factor, 0.2), 0.8)

        old_u_len = u_len
        old_t_len = t_len

        if random.random() < win_prob:
            gain = random.randint(0, 3)
            loss = random.randint(1, 2)
            updated_user = {
                'length': user_data['length'] + gain
            }
            updated_target = {
                'length': max(1, target_data['length'] - loss)
            }
            self.update_user_data(group_id, user_id, updated_user)
            self.update_user_data(group_id, target_id, updated_target)
            # 修正：使用 winner 和 loser 参数
            text = random.choice(self.niuniu_texts['compare']['win']).format(
                winner=nickname,
                loser=target_data['nickname'],
                gain=gain
            )
            total_gain = gain
            if (self.shop.get_user_items(group_id, user_id).get("淬火爪刀", 0) > 0 
                and abs(u_len - t_len) > 10 
                and u_len < t_len):
                extra_loot = int(target_data['length'] * 0.1)
                updated_user = {
                    'length': user_data['length'] + gain + extra_loot
                }
                self.update_user_data(group_id, user_id, updated_user)
                total_gain += extra_loot
                text += f"\n🔥 淬火爪刀触发！额外掠夺 {extra_loot}cm！"
                self.shop.consume_item(group_id, user_id, "淬火爪刀")  

            if abs(u_len - t_len) >= 20 and user_data['hardness'] < target_data['hardness']:
                extra_gain = random.randint(0, 5)
                updated_user = {
                    'length': user_data['length'] + gain + extra_gain
                }
                self.update_user_data(group_id, user_id, updated_user)
                total_gain += extra_gain
                text += f"\n🎁 由于极大劣势获胜，额外增加 {extra_gain}cm！"
            if abs(u_len - t_len) > 10 and u_len < t_len:
                stolen_length = int(target_data['length'] * 0.2)
                updated_user = {
                    'length': user_data['length'] + gain + stolen_length
                }
                updated_target = {
                    'length': max(1, target_data['length'] - loss - stolen_length)
                }
                self.update_user_data(group_id, user_id, updated_user)
                self.update_user_data(group_id, target_id, updated_target)
                total_gain += stolen_length
                text += f"\n🎉 {nickname} 掠夺了 {stolen_length}cm！"
            if abs(u_len - t_len) <= 5 and user_data['hardness'] > target_data['hardness']:
                text += f"\n🎉 {nickname} 因硬度优势获胜！"
            if total_gain == 0:
                text += f"\n{self.niuniu_texts['compare']['user_no_increase'].format(nickname=nickname)}"
        else:
            gain = random.randint(0, 3)
            loss = random.randint(1, 2)
            updated_target = {
                'length': target_data['length'] + gain
            }
            if self.shop.consume_item(group_id, user_id, "余震"):
                result_msg = [f"🛡️ 【余震生效】{nickname} 未减少长度！"]
                self.update_user_data(group_id, target_id, updated_target)
            else:
                updated_user = {
                    'length': max(1, user_data['length'] - loss)
                }
                updated_target = {
                    'length': target_data['length'] + gain
                }
                self.update_user_data(group_id, user_id, updated_user)
                self.update_user_data(group_id, target_id, updated_target)
                result_msg = [f"💔 {nickname} 减少 {loss}cm"]
            # 修正：使用 winner 和 loser 参数
            text = random.choice(self.niuniu_texts['compare']['lose']).format(
                loser=nickname,
                winner=target_data['nickname'],
                loss=loss
            )
        # 硬度衰减
        if random.random() < 0.3:
            updated_user = {
                'hardness': max(1, user_data['hardness'] - 1)
            }
            self.update_user_data(group_id, user_id, updated_user)
        if random.random() < 0.3:
            updated_target = {
                'hardness': max(1, target_data['hardness'] - 1)
            }
            self.update_user_data(group_id, target_id, updated_target)

        user_data = self.get_user_data(group_id, user_id)
        target_data = self.get_user_data(group_id, target_id)
        result_msg = [
            "⚔️ 【牛牛对决结果】 ⚔️",
            f"🗡️ {nickname}: {self.format_length(old_u_len)} → {self.format_length(user_data['length'])}",
            f"🛡️ {target_data['nickname']}: {self.format_length(old_t_len)} → {self.format_length(target_data['length'])}",
            f"📢 {text}"
        ]
        special_event_triggered = False
        if abs(u_len - t_len) <= 5 and random.random() < 0.075:
            result_msg.append("💥 双方势均力敌！")
            special_event_triggered = True
        if not special_event_triggered and (user_data['hardness'] <= 2 or target_data['hardness'] <= 2) and random.random() < 0.05:
            original_user_len = user_data['length']
            original_target_len = target_data['length']
            
            updated_user = {
                'length': max(1, original_user_len // 2)
            }
            updated_target = {
                'length': max(1, original_target_len // 2)
            }
            self.update_user_data(group_id, user_id, updated_user)
            self.update_user_data(group_id, target_id, updated_target)
            
            if self.shop.get_user_items(group_id, user_id).get("妙脆角", 0) > 0:
                updated_user = {
                    'length': original_user_len
                }
                self.update_user_data(group_id, user_id, updated_user)
                result_msg.append(f"🛡️ {nickname} 的妙脆角生效，防止了长度减半！")
                self.shop.consume_item(group_id, user_id, "妙脆角")
            
            if self.shop.get_user_items(group_id, target_id).get("妙脆角", 0) > 0:
                updated_target = {
                    'length': original_target_len
                }
                self.update_user_data(group_id, target_id, updated_target)
                result_msg.append(f"🛡️ {target_data['nickname']} 的妙脆角生效，防止了长度减半！")
                self.shop.consume_item(group_id, target_id, "妙脆角")
            
            result_msg.append("双方牛牛因过于柔软发生缠绕！")
            special_event_triggered = True

        if not special_event_triggered and abs(u_len - t_len) < 10 and random.random() < 0.025:
            original_user_len = user_data['length']
            original_target_len = target_data['length']
            
            updated_user = {
                'length': max(1, original_user_len // 2)
            }
            updated_target = {
                'length': max(1, original_target_len // 2)
            }
            self.update_user_data(group_id, user_id, updated_user)
            self.update_user_data(group_id, target_id, updated_target)
            
            if self.shop.get_user_items(group_id, user_id).get("妙脆角", 0) > 0:
                updated_user = {
                    'length': original_user_len
                }
                self.update_user_data(group_id, user_id, updated_user)
                result_msg.append(f"🛡️ {nickname} 的妙脆角生效，防止了长度减半！")
                self.shop.consume_item(group_id, user_id, "妙脆角")
            
            if self.shop.get_user_items(group_id, target_id).get("妙脆角", 0) > 0:
                updated_target = {
                    'length': original_target_len
                }
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

        group_data = self.get_group_data(group_id)
        if not group_data.get('plugin_enabled', False):
            yield event.plain_result("❌ 插件未启用")
            return

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

        hardness = user_data['hardness']

        text = self.niuniu_texts['my_niuniu']['info'].format(
            nickname=nickname,
            length=length_str,
            hardness=hardness,
            evaluation=evaluation
        )
        yield event.plain_result(text)

    async def _show_ranking(self, event):
        group_id = str(event.message_obj.group_id)
        group_data = self.get_group_data(group_id)
        if not group_data.get('plugin_enabled', False):
            yield event.plain_result("❌ 插件未启用")
            return

        data = self._load_niuniu_lengths()
        group_data = data.get(group_id, {'plugin_enabled': False})
        valid_users = [
            (uid, data) for uid, data in group_data.items()
            if isinstance(data, dict) and 'length' in data
        ]

        if not valid_users:
            yield event.plain_result(self.niuniu_texts['ranking']['no_data'])
            return

        sorted_users = sorted(valid_users, key=lambda x: x[1]['length'], reverse=True)[:10]

        ranking = [self.niuniu_texts['ranking']['header']]
        for idx, (uid, data) in enumerate(sorted_users, 1):
            ranking.append(
                self.niuniu_texts['ranking']['item'].format(
                    rank=idx,
                    name=data['nickname'],
                    length=self.format_length(data['length'])
                )
            )

        yield event.plain_result("\n".join(ranking))

    async def _show_menu(self, event):
        yield event.plain_result(self.niuniu_texts['menu']['default'])
    # endregion
