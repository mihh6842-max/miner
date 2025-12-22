import asyncio
import logging
import aiosqlite
import datetime
import re
import random
import uuid
import os
import math
import time
from decimal import Decimal, getcontext
from aiogram import Bot, Dispatcher, Router, F
from aiogram.types import Message, CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, KeyboardButton
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError

TOKEN = "7574504052:AAGuScWo3tKbj_NvT7B28LT-wCQXUhw75vE"
PCCLUB = -1003246180665 
ADMIN = [5929120983, 963551489, 8315604670, 7453830377, 7338817463]
PAYMENT_TOKEN = "goida"

DB_FILE = "2pcclub.db"

prices = [
    [1, 5, 3600],
    [2, 7.5, 5400],
    [3, 10, 7200],
    [4, 12.5, 9000],
    [5, 17.5, 12600],
    [6, 25, 18000],
    [7, 35, 25200],
    [8, 50, 36000],
    [9, 62.5, 45000],
    [10, 75, 54000],
    [11, 85, 61200],
    [12, 100, 72000],
    [13, 150, 108000],
    [14, 200, 144000],
    [15, 250, 180000],
    [16, 350, 252000],
    [17, 500, 360000],
    [18, 600, 432000],
    [19, 825, 594000],
    [20, 1150, 1928205],
    [21, 1500, 2515050],
    [22, 2000, 3353400],
    [23, 2150, 3604905],
    [24, 2400, 4024080],
    [25, 2750, 4610925],
    [26, 3300, 5533110],
    [27, 3750, 6287625],
    [28, 4400, 7377480],
    [29, 5000, 8383500],
    [30, 5800, 9724860],
    [31, 6900, 11569230],
    [32, 8000, 13413600],
    [33, 9500, 15921225],
    [34, 11000, 18443700],
    [35, 13900, 23306130],
    [36, 17000, 28488900],
    [37, 20500, 34373925],
    [38, 23700, 39737790],
    [39, 27000, 45270900],
    [40, 31500, 52816050],
    [41, 38500, 64552950],
    [42, 45000, 75451500],
    [43, 52500, 88026750],
    [44, 61000, 102278700],
    [45, 69000, 115692300],
    [46, 79000, 132444300],
    [47, 90000, 150753000],
    [48, 100000, 167670000],
    [49, 110000, 184437000],
    [50, 120000, 201204000]
]


prices_expansion = [
    [51, 132000, 221316840], 
    [52, 145200, 243456840], 
    [53, 159720, 267802365], 
    [54, 175692, 294582776], 
    [55, 193261, 324041068],
    [56, 212587, 356443507], 
    [57, 233846, 392089674], 
    [58, 257230, 431298645], 
    [59, 282953, 474409518],
    [60, 311248, 521721356],
    [61, 342373, 573908491], 
    [62, 376610, 631614340], 
    [63, 414271, 694610788], 
    [64, 455698, 764071850], 
    [65, 501268, 840479034],
    [66, 551395, 924503650], 
    [67, 606535, 1018281498], 
    [68, 667188, 1118008915], 
    [69, 733907, 1229049599], 
    [70, 807298, 1350960959],

    # 3 экспансия открывает:
    [71, 888028, 1488973323], 
    [72, 976831, 1637703974], 
    [73, 1074514, 1798638117], 
    [74, 1181966, 1981803400], 
    [75, 1299971, 2180058893],
    [76, 1429968, 2391771583], 
    [77, 1572965, 2648345726], 
    [78, 1730262, 2907362100], 
    [79, 1903289, 3203505825], 
    [80, 2093617, 3530620000],

    # 4 экспансия открывает:
    [81, 2302979, 3898466049], 
    [82, 2533277, 4300818204], 
    [83, 2786604, 4745898621],
    [84, 3065264, 5233673484], 
    [85, 3371790, 5773956250],
    [86, 3708969, 6367009707], 
    [87, 4079866, 7017157073], 
    [88, 4487853, 7729662656], 
    [89, 4936639, 8510926311], 
    [90, 5430303, 9363153775],

    # 5 экспансия открывает:
    [91, 5973333, 10181062125], 
    [92, 6570666, 11191416068], 
    [93, 7227732, 12306000000], 
    [94, 7950505, 13497233964], 
    [95, 8745556, 14873884630],
    [96, 9620112, 16361331944], 
    [97, 10582123, 17985837511], 
    [98, 11640335, 19811142147], 
    [99, 12804368, 21815693892], 
    [100, 14084805, 24012100000],

    # 6 экспансия открывает:
    [101, 15493286, 26090242800], 
    [102, 17042615, 28582857700], 
    [103, 18746876, 31434524062], 
    [104, 20621564, 34577161062], 
    [105, 22683720, 38033610000],
    [106, 24952092, 41836971750], 
    [107, 27447294, 46020000000], 
    [108, 30192023, 50637375000], 
    [109, 33211225, 55675755000], 
    [110, 36532347, 61241467500],

    # 7 экспансия открывает:
    [111, 40185581, 67149851250], 
    [112, 44204139, 74261250000], 
    [113, 48624553, 81510757500], 
    [114, 53486968, 89661532500], 
    [115, 58835665, 98627220000],
    [116, 64719231, 108509133750], 
    [117, 71191154, 119343780000], 
    [118, 78310270, 131266980000], 
    [119, 86141297, 144394143750], 
    [120, 94755427, 158833143750],

    # 8 экспансия открывает:
    [121, 104230969, 174712140000], 
    [122, 114654066, 192004650000], 
    [123, 126119472, 210635726250], 
    [124, 138731419, 232516082500], 
    [125, 152604561, 255755043750],
    [126, 167865017, 281331630000], 
    [127, 184646700, 309488696250], 
    [128, 203088000, 340485787500], 
    [129, 223400000, 374602725000], 
    [130, 245740000, 412188750000],
# 9 экспансия открывает:
    [131, 270314000, 453407625000], 
    [132, 297345400, 498613387500], 
    [133, 327079940, 549305550000], 
    [134, 360000000, 604277475000], 
    [135, 396000000, 663125250000],
    [136, 435600000, 731176275000], 
    [137, 479160000, 804187237500], 
    [138, 527076000, 884585857500], 
    [139, 579783600, 973240271250], 
    [140, 637761960, 1070775037500],
    
    [141, 701538156, 1178926329000],
    [142, 771691972, 1297410750000], 
    [143, 848861169, 1428222375000], 
    [144, 933747285, 1571044500000], 
    [145, 1027122014, 1727350312500],
    [146, 1129834215, 1899146200000], 
    [147, 1242817637, 2089408412500], 
    [148, 1367099401, 2298909225000], 
    [149, 1503809341, 2527514216250], 
    [150, 1654190275, 2781907500000]
]

taxes = [
    (1, 10000), (2, 20000), (3, 30000), (4, 40000), (5, 50000),
    (6, 60000), (7, 70000), (8, 80000), (9, 90000), (10, 100000),
    (11, 125000), (12, 150000), (13, 175000), (14, 200000), (15, 250000),
    (16, 300000), (17, 350000), (18, 400000), (19, 450000), (20, 500000),
    (21, 600000), (22, 700000), (23, 800000), (24, 900000), (25, 1000000),
    (26, 1250000), (27, 1500000), (28, 1750000), (29, 2000000), (30, 2500000),
    (31, 3000000), (32, 3500000), (33, 4000000), (34, 5000000), (35, 6000000),
    (36, 7000000), (37, 10000000), (38, 15000000), (39, 20000000), (40, 30000000),
    (41, 40000000), (42, 50000000), (43, 60000000), (44, 70000000), (45, 80000000),
    (46, 90000000), (47, 100000000), (48, 125000000), (49, 150000000), (50, 250000000)
]

taxes_expansion = [
    (51, 300000000), (52, 350000000), (53, 400000000), (54, 450000000), (55, 500000000),
    (56, 600000000), (57, 700000000), (58, 800000000), (59, 900000000), (60, 1000000000),
    (61, 1250000000), (62, 1500000000), (63, 1750000000), (64, 2000000000), (65, 2500000000),
    (66, 3000000000), (67, 3500000000), (68, 4000000000), (69, 4500000000), (70, 5000000000),
    (71, 6000000000), (72, 7000000000), (73, 8000000000), (74, 9000000000), (75, 10000000000),
    (76, 12500000000), (77, 15000000000), (78, 17500000000), (79, 20000000000), (80, 25000000000),
    (81, 30000000000), (82, 35000000000), (83, 40000000000), (84, 50000000000), (85, 60000000000),
    (86, 70000000000), (87, 80000000000), (88, 90000000000), (89, 100000000000), (90, 125000000000),
    (91, 150000000000), (92, 175000000000), (93, 200000000000), (94, 250000000000), (95, 300000000000),
    (96, 350000000000), (97, 400000000000), (98, 450000000000), (99, 500000000000), (100, 600000000000),
    (101, 700000000000), (102, 800000000000), (103, 900000000000), (104, 1000000000000), (105, 1250000000000),
    (106, 1500000000000), (107, 1750000000000), (108, 2000000000000), (109, 2500000000000), (110, 3000000000000),
    (111, 3500000000000), (112, 4000000000000), (113, 4500000000000), (114, 5000000000000), (115, 6000000000000),
    (116, 7000000000000), (117, 8000000000000), (118, 9000000000000), (119, 10000000000000), (120, 12500000000000),
    (121, 15000000000000), (122, 17500000000000), (123, 20000000000000), (124, 25000000000000), (125, 30000000000000),
    (126, 35000000000000), (127, 40000000000000), (128, 45000000000000), (129, 50000000000000), (130, 60000000000000),
    (131, 70000000000000), (132, 80000000000000), (133, 90000000000000), (134, 100000000000000), (135, 125000000000000),
    (136, 150000000000000), (137, 175000000000000), (138, 200000000000000), (139, 250000000000000), (140, 300000000000000),
    (141, 350000000000000), (142, 400000000000000), (143, 450000000000000), (144, 500000000000000), (145, 600000000000000),
    (146, 700000000000000), (147, 800000000000000), (148, 900000000000000), (149, 1000000000000000), (150, 1250000000000000)
]

ads = [
    (1, "Баннер на сайте", 100000, 5, 3, 6),
    (2, "Реклама в соцсетях", 250000, 12.5, 8, 16),
    (3, "ТВ-реклама", 550000, 25, 18, 36),
    (4, "Радио-реклама", 1000000, 30, 24, 48),
    (5, "Газетная реклама", 2500000, 35, 48, 48)  
]

upgrade = [
    (1, 25000), (2, 50000), (3, 100000), (4, 250000), (5, 500000),
]


update = [
    (2, 3600, 20),
    (3, 10800, 30),
    (4, 21600, 48),
    (5, 36000, 68),
    (6, 63000, 95),
    (7, 90720, 130),
    (8, 131040, 175),
    (9, 184320, 235),
    (10, 248400, 300),
    (11, 334800, 375),
    (12, 442800, 460),
    (13, 583200, 560),
    (14, 756000, 680),
    (15, 972000, 820),
    (16, 1263600, 1000),
    (17, 1634400, 1250),
    (18, 2073600, 1500),
    (19, 2689200, 1850),
    (20, 3445200, 2200),
    (21, 6609600.0, 5300),
    (22, 8488800.0, 6400),
    (23, 10454400.0, 7700),
    (24, 13111200.0, 9200),
    (25, 16200000.0, 11000),
    (26, 20120400.0, 13400),
    (27, 24840000.0, 16000),
    (28, 30844800.0, 19000),
    (29, 38264400.0, 23000),
    (30, 46980000.0, 28000),
    (31, 58320000.0, 34000),
    (32, 71604000.0, 42000),
    (33, 89586000.0, 52000),
    (34, 111348000.0, 64000),
    (35, 140328000.0, 80000),
    (36, 174960000.0, 100000),
    (37, 219240000.0, 126000),
    (38, 275400000.0, 158000),
    (39, 344520000.0, 198000),
    (40, 434160000.0, 250000),
    (41, 545400000.0, 320000),
    (42, 683640000.0, 400000),
    (43, 856980000.0, 500000),
    (44, 1073250000.0, 640000),
    (45, 1336500000.0, 800000),
    (46, 1681560000.0, 1000000),
    (47, 2110320000.0, 1260000),
    (48, 2646000000.0, 1600000),
    (49, 3326400000.0, 2000000),
    (50, 4183200000.0, 2500000)
]

update_expansion = [
    # Экспансия 1
    (51, 5832000000.0, 2640000), 
    (52, 6415200000.0, 2904000), 
    (53, 7056720000.0, 3194400), 
    (54, 7762392000.0, 3513840), 
    (55, 8538631200.0, 3865220),
    (56, 9392494320.0, 4251740), 
    (57, 10331743752.0, 4676920), 
    (58, 11364918127.5, 5144600), 
    (59, 12501410000.0, 5659060), 
    (60, 13751550933.0, 6224960),

    # Экспансия 2
    (61, 15126706026.0, 6847460), 
    (62, 16639376628.0, 7532200), 
    (63, 18303314290.5, 8285420), 
    (64, 20133645718.5, 9113960), 
    (65, 22147010290.5, 10025360),
    (66, 24361711320.0, 11027900), 
    (67, 26800000000.0, 12130700), 
    (68, 29475330696.0, 13343760), 
    (69, 32417685613.5, 14678140), 
    (70, 35654954175.0, 16145960),

    # Экспансия 3
    (71, 39220449592.5, 17760560), 
    (72, 43142494551.0, 19536620), 
    (73, 47456744005.5, 21490280), 
    (74, 52202418406.5, 23639320), 
    (75, 57422660247.0, 25999420),
    (76, 63164926270.5, 28599360), 
    (77, 69479700000.0, 31459300), 
    (78, 76424700000.0, 34605240), 
    (79, 84067050000.0, 38065780), 
    (80, 92475450000.0, 41872340),

    # Экспансия 4
    (81, 101737950000.0, 46059580), 
    (82, 111955350000.0, 50665540), 
    (83, 123231450000.0, 55732080), 
    (84, 135686400000.0, 61305280), 
    (85, 149446350000.0, 67435800),
    (86, 164673000000.0, 74179380), 
    (87, 181341000000.0, 81597320), 
    (88, 199554000000.0, 89757060), 
    (89, 219469500000.0, 98732780), 
    (90, 241290000000.0, 108606060),

    # Экспансия 5
    (91, 265234500000.0, 119466660), 
    (92, 291510000000.0, 131413320), 
    (93, 320340000000.0, 144554640), 
    (94, 351999999999.0, 159010100), 
    (95, 386775000000.0, 174911120),
    (96, 424995000000.0, 192402240), 
    (97, 467055000000.0, 211642460), 
    (98, 513300000000.0, 232806700), 
    (99, 564060000000.0, 256087360), 
    (100, 619725000000.0, 281696100),

    # Экспансия 6
    (101, 680655000000.0, 309865720), 
    (102, 747450000000.0, 340852300), 
    (103, 820800000000.0, 374937520), 
    (104, 901500000000.0, 412431280), 
    (105, 990150000000.0, 453674400),
    (106, 1087950000000.0, 499041840), 
    (107, 1195950000000.0, 548945880), 
    (108, 1315350000000.0, 603840460), 
    (109, 1447050000000.0, 664224500), 
    (110, 1592550000000.0, 730646940),

    # Экспансия 7
    (111, 1752750000000.0, 803711620), 
    (112, 1930000000000.0, 884082780), 
    (113, 2125500000000.0, 972491060), 
    (114, 2341050000000.0, 1069739360), 
    (115, 2578350000000.0, 1176713300),
    (116, 2839200000000.0, 1294384620), 
    (117, 3125850000000.0, 1423823080), 
    (118, 3440850000000.0, 1566205400), 
    (119, 3786900000000.0, 1722825940), 
    (120, 4167450000000.0, 1895108540),

    # Экспансия 8
    (121, 4585650000000.0, 2084619380),

    (122, 5045400000000.0, 2293081320), 
    (123, 5550000000000.0, 2522389440), 
    (124, 6103950000000.0, 2774628380), 
    (125, 6712050000000.0, 3052091220),
    (126, 7380300000000.0, 3357300340), 
    (127, 8115000000000.0, 3692934000), 
    (128, 8923350000000.0, 4061760000), 
    (129, 9813150000000.0, 4468000000), 
    (130, 10793700000000.0, 4914800000),

# Экспансия 9
    (131, 11873250000000.0, 5406280000), 
    (132, 13060500000000.0, 5946908000), 
    (133, 14365200000000.0, 6541598800), 
    (134, 15797250000000.0, 7200000000), 
    (135, 17369100000000.0, 7920000000),
    (136, 19093500000000.0, 8712000000), 
    (137, 20983950000000.0, 9583200000), 
    (138, 23054400000000.0, 10541520000), 
    (139, 25322550000000.0, 11595672000), 
    (140, 27808350000000.0, 12755239200),

    # Экспансия 10
    (141, 30532350000000.0, 14030763120),
    (142, 33520050000000.0, 15433839440), 
    (143, 36802500000000.0, 16977223380), 
    (144, 40415700000000.0, 18674945700), 
    (145, 44397300000000.0, 20542440280),
    (146, 48787650000000.0, 22596684300), 
    (147, 53631750000000.0, 24856352740), 
    (148, 58978200000000.0, 27341988020), 
    (149, 64873500000000.0, 30076186820), 
    (150, 71367000000000.0, 33083805500)
]

BOOSTER_TYPES = {
    "income": {
        "name": "📈 Бустер дохода",
        "bonus": 0.25,  # +25% к доходу
        "description": "+25% к грязному доходу"
    },
    "auto": {
        "name": "🤖 Автоматизация",
        "description": "Автоматическая оплата налогов и работа"
    },
    "premium": {
        "name": "👑 PREMIUM Статус",
        "bonus": 0.35,  # +35% к доходу фермы
        "description": "PREMIUM статус с бонусами"
    }
}

EXPANSION_STAGES = [
    "Новичок",
    "Подвальный ПК клуб", 
    "Фриланс-Хаб",
    "Спальный район",
    "Клуб в ТЦ",
    "Эпоха модернизации",
    "Студенческий кампус",
    "Клуб в центре города",
    "Филиалы в сёлах",
    "Клуб в столице",
    "Сеть клубов по стране"
]

WORK_JOBS = [
    {"id": 1, "name": "Техно-менеджер", "reward": 50, "min_exp": 0, "max_exp": 100},
    {"id": 2, "name": "Киберапгрейдер", "reward": 100, "min_exp": 100, "max_exp": 200},
    {"id": 3, "name": "Мастер цифрового развития", "reward": 200, "min_exp": 200, "max_exp": 300},
    {"id": 4, "name": "Администратор прокачки", "reward": 400, "min_exp": 300, "max_exp": 400},
    {"id": 5, "name": "Гей-оптимизатор", "reward": 800, "min_exp": 400, "max_exp": 500},
    {"id": 6, "name": "Техноэволюционер", "reward": 1500, "min_exp": 500, "max_exp": 600},
    {"id": 7, "name": "Апгрейд-консультант", "reward": 2500, "min_exp": 600, "max_exp": 700},
    {"id": 8, "name": "Директор ПК центра", "reward": 4000, "min_exp": 700, "max_exp": 800},
    {"id": 9, "name": "Диджитал-стратег", "reward": 6000, "min_exp": 800, "max_exp": 900},
    {"id": 10, "name": "Мастер гейской эволюции", "reward": 9000, "min_exp": 900, "max_exp": 1000},
    {"id": 11, "name": "Техно-архитектор", "reward": 12500, "min_exp": 1000, "max_exp": 1100},
    {"id": 12, "name": "Клубный модернизатор", "reward": 17000, "min_exp": 1100, "max_exp": 1200},
    {"id": 13, "name": "Киберинженер", "reward": 22000, "min_exp": 1200, "max_exp": 1300},
    {"id": 14, "name": "Эксперт игровых ПК", "reward": 28000, "min_exp": 1300, "max_exp": 1400},
    {"id": 15, "name": "Сборщик ПК", "reward": 35000, "min_exp": 1400, "max_exp": 1500},
    {"id": 16, "name": "Мастер апгрейда", "reward": 42000, "min_exp": 1500, "max_exp": 1600},
    {"id": 17, "name": "Стратег цифрового роста", "reward": 50000, "min_exp": 1600, "max_exp": 1700},
    {"id": 18, "name": "Киберклубный", "reward": 58000, "min_exp": 1700, "max_exp": 1800},
    {"id": 19, "name": "Технический Визионер", "reward": 65000, "min_exp": 1800, "max_exp": 1900},
    {"id": 20, "name": "Бог компьютеров", "reward": 72000, "min_exp": 1900, "max_exp": 2000}
]

# Event configuration
EVENTS = [
    {
        "type": "streamer",
        "name": "👨‍💻 Стример",
        "min_percent": 5,
        "max_percent": 15,
        "min_hours": 1,
        "max_hours": 3,
        "weight": 70  # 70% шанс
    },
    {
        "type": "blogger", 
        "name": "🤳 Блогер",
        "min_percent": 20,
        "max_percent": 30,
        "min_hours": 1,
        "max_hours": 3,
        "weight": 30  # 30% шанс
    }
]




# Добавляем в CONFIGURATION
REPUTATION_LEVELS = [
    {"level": 1, "name": "Новичок клуба", "points_required": 0, "income_bonus": 0.0, "tax_reduction": 0.0},
    {"level": 2, "name": "Опытный арендатор", "points_required": 10000, "income_bonus": 0.025, "tax_reduction": 0.01},
    {"level": 3, "name": "Младший менеджер", "points_required": 30000, "income_bonus": 0.05, "tax_reduction": 0.02},
    {"level": 4, "name": "Эксперт ПК", "points_required": 70000, "income_bonus": 0.075, "tax_reduction": 0.03},
    {"level": 5, "name": "Старший инвестор", "points_required": 150000, "income_bonus": 0.10, "tax_reduction": 0.04},
    {"level": 6, "name": "Мастер клуба", "points_required": 300000, "income_bonus": 0.125, "tax_reduction": 0.05},
    {"level": 7, "name": "Кибер-Легенда", "points_required": 550000, "income_bonus": 0.15, "tax_reduction": 0.06},
    {"level": 8, "name": "Техно-титан", "points_required": 900000, "income_bonus": 0.175, "tax_reduction": 0.07},
    {"level": 9, "name": "Глобальный Сетевик", "points_required": 1500000, "income_bonus": 0.20, "tax_reduction": 0.08},
    {"level": 10, "name": "Монополист клубов", "points_required": 2500000, "income_bonus": 0.25, "tax_reduction": 0.10}
]

ROOM_NAMES = {
    1: "Начальное помещение",
    2: "Аренда подвала", 
    3: "Складской уголок",
    4: "Офис 10 кв.м",
    5: "Маленький кабинет",
    6: "Комната в общежитии",
    7: "Склад",
    8: "Комната в ТЦ",
    9: "Клуб 'Первый lvl'",
    10: "Офис 20 кв.м",
    11: "Студия 'Геймер'",
    12: "Лофт-студия",
    13: "Аренда помещения",
    14: "Клуб 'UpTime'",
    15: "Кабинет",
    16: "Офис 32 кв.м",
    17: "Игровой зал",
    18: "Лаунж-Зона",
    19: "Клуб '24/7'",
    20: "Коворкинг",
    21: "Аренда ТЦ",
    22: "Игровая студия",
    23: "Кафе с ПК",
    24: "Клуб 'CuberBery'",
    25: "Офис 50 кв.м",
    26: "Зона 'ExtraCoffe'",
    27: "Гостинница с ПК",
    28: "Офис 65 кв.м",
    29: "Филиал 'Запад'",
    30: "Клуб 'ТехноБлейд'",
    31: "Комплекст 'Data Stream'",
    32: "Геймерский штаб",
    33: "Сеть заведений",
    34: "Платформа 'Грид'",
    35: "Офис 85 кв.м",
    36: "Небольшая студия",
    37: "Клуб 'Профи'",
    38: "Техно-башня",
    39: "Штаб-квартира",
    40: "Студия",
    41: "Офис 100 кв.м",
    42: "Филиал 'Восток'",
    43: "Корпоративный блок",
    44: "Большая студия",
    45: "Клуб 'VIP'",
    46: "ТЦ",
    47: "Межрегиональный дата-центр",
    48: "Комплекс 'Game'",
    49: "Монополия",
    50: "Монополия"
}

for i in range(51, 151):
    expansion_num = (i - 1) // 10
    ROOM_NAMES[i] = f"Экспансия {expansion_num} - Уровень {i}"

getcontext().prec = 50

# ===== LOGGING SETUP =====
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("bot.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ===== DATABASE CONNECTION POOL =====
class Database:
    _instance = None
    _conn = None
    
    @classmethod
    async def get_connection(cls):
        if cls._conn is None:
            cls._conn = await aiosqlite.connect(DB_FILE, check_same_thread=False)
            cls._conn.row_factory = aiosqlite.Row
        return cls._conn
    
    @classmethod
    async def close(cls):
        if cls._conn:
            await cls._conn.close()
            cls._conn = None
# ===== DATABASE FUNCTIONS =====


async def update_database_schema():
    """Обновляем схему базы данных для поддержки экспансий и бустеров"""
    conn = await Database.get_connection()
    
    # Проверяем существование колонок для бустеров
    try:
        # Проверяем существование колонки income_booster_end
        await conn.execute('SELECT income_booster_end FROM stats LIMIT 1')
    except Exception:
        # Добавляем колонку если её нет
        await conn.execute('ALTER TABLE stats ADD COLUMN income_booster_end TIMESTAMP')
        logger.info("Added income_booster_end column to stats table")
    
    try:
        # Проверяем существование колонки auto_booster_end  
        await conn.execute('SELECT auto_booster_end FROM stats LIMIT 1')
    except Exception:
        # Добавляем колонку если её нет
        await conn.execute('ALTER TABLE stats ADD COLUMN auto_booster_end TIMESTAMP')
        logger.info("Added auto_booster_end column to stats table")
    
    try:
        # Проверяем существование колонки expansion_level
        await conn.execute('SELECT expansion_level FROM stats LIMIT 1')
    except Exception:
        # Добавляем колонку если её нет
        await conn.execute('ALTER TABLE stats ADD COLUMN expansion_level INTEGER DEFAULT 0')
        logger.info("Added expansion_level column to stats table")
    
    await conn.commit()


async def init_db():
    """Initialize SQLite database with required tables"""
    conn = await Database.get_connection()
    
    await conn.execute('''
        CREATE TABLE IF NOT EXISTS stats (
            userid INTEGER PRIMARY KEY,
            bal NUMERIC DEFAULT 5000,  -- ИЗМЕНЕНО: стартовый баланс 5000$
            room INTEGER DEFAULT 1,
            pc INTEGER DEFAULT 0,
            bonus INTEGER DEFAULT 1,
            income NUMERIC DEFAULT 0,
            reg_day TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            network INTEGER,
            username TEXT,
            name TEXT DEFAULT 'Никнейм не указан',
            all_wallet NUMERIC DEFAULT 0,
            premium TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            ref  INTEGER,
            net_inc NUMERIC DEFAULT 0,
            title TEXT,
            upgrade_internet INTEGER DEFAULT 0,
            upgrade_devices INTEGER DEFAULT 0,
            upgrade_interior INTEGER DEFAULT 0,
            upgrade_minibar INTEGER DEFAULT 0,
            upgrade_service INTEGER DEFAULT 0,
            taxes NUMERIC DEFAULT 0,
            all_pcs INTEGER DEFAULT 0,
            max_bal NUMERIC DEFAULT 0,
            tickets INTEGER DEFAULT 1,
            active_ticket BOOLEAN DEFAULT 0,
            income_booster_end TIMESTAMP,  
            auto_booster_end TIMESTAMP,
            expansion_level INTEGER DEFAULT 0  -- НОВОЕ: уровень экспансии
        )
    ''')
    
    await conn.execute('''
        CREATE TABLE IF NOT EXISTS user_events (
            user_id INTEGER PRIMARY KEY,
            event_type TEXT,
            bonus_percent INTEGER,
            end_time TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    await conn.execute('''
        CREATE TABLE IF NOT EXISTS networks (
            name TEXT DEFAULT 'Название не установлено',
            owner_id INTEGER PRIMARY KEY,
            description TEXT DEFAULT 'Описание не установлено',
            income NUMERIC DEFAULT 0,
            requests TEXT DEFAULT '[]',
            type TEXT DEFAULT 'open',
            ban_users TEXT DEFAULT '[]',
            mailing TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            admins TEXT DEFAULT '[]'
        )
    ''')
    
    await conn.execute('''
        CREATE TABLE IF NOT EXISTS banned_franchise_users (
            user_id INTEGER PRIMARY KEY,
            banned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            banned_by INTEGER,
            reason TEXT DEFAULT "Запрет на создание франшиз"
        )
    ''')    
    
    await conn.execute('''
        CREATE TABLE IF NOT EXISTS pc (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            userid INTEGER,
            lvl INTEGER,
            income NUMERIC
        )
    ''')
    
    await conn.execute('''
        CREATE TABLE IF NOT EXISTS actions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            dt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            action TEXT,
            userid INTEGER
        )
    ''')
    
    await conn.execute('''
        CREATE TABLE IF NOT EXISTS orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            userid INTEGER,
            label TEXT,
            product TEXT,
            success INTEGER DEFAULT 0,
            amount INTEGER,
            days INTEGER,
            paid_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    await conn.execute('''
        CREATE TABLE IF NOT EXISTS promos (
            name TEXT PRIMARY KEY,
            use INTEGER DEFAULT 0,
            use_max INTEGER,
            users TEXT DEFAULT '[]',
            reward TEXT,
            quantity INTEGER
        )
    ''')
    
    await conn.execute('''
        CREATE TABLE IF NOT EXISTS titles (
            name TEXT,
            users TEXT DEFAULT '[]',
            id TEXT PRIMARY KEY
        )
    ''')
    
    await conn.execute('''
        CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            msg_text TEXT,
            user_from INTEGER,
            chat_id INTEGER,
            msg_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    

    await conn.execute('''
        CREATE TABLE IF NOT EXISTS user_reputation (
            user_id INTEGER PRIMARY KEY,
            reputation_points INTEGER DEFAULT 0,
            reputation_level INTEGER DEFAULT 1,
            total_earned_reputation INTEGER DEFAULT 0
        )
    ''')
    
    await conn.execute('''
        CREATE TABLE IF NOT EXISTS chats (
            chat_id INTEGER PRIMARY KEY,
            users TEXT DEFAULT '[]',
            date_create TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    await conn.execute('''
        CREATE TABLE IF NOT EXISTS ads (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            userid INTEGER,
            num INTEGER,
            percent INTEGER,
            dt TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    await conn.execute('''
        CREATE TABLE IF NOT EXISTS user_social_bonus (
            user_id INTEGER PRIMARY KEY,
            channel_subscribed BOOLEAN DEFAULT FALSE,
            chat_subscribed BOOLEAN DEFAULT FALSE,
            bio_checked BOOLEAN DEFAULT FALSE,
            last_check TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(user_id) REFERENCES stats(userid)
        )
    ''')
    
    await conn.execute('''
        CREATE TABLE IF NOT EXISTS user_work_stats (
            user_id INTEGER PRIMARY KEY,
            exp INTEGER DEFAULT 0,
            last_work TEXT,
            total_earned REAL DEFAULT 0
        )
    ''')
    
    await conn.commit()
    
    
    

    
    
def parse_array(text):
    """Parse array from string format"""
    if text == '[]' or not text:
        return []
    try:
        return [int(x) for x in text.strip('[]').split(',') if x.strip()]
    except (ValueError, TypeError):
        return []

def format_array(arr):
    """Format array to string for storage"""
    if not arr:
        return '[]'
    return '[' + ','.join(map(str, arr)) + ']'

async def update_data(username, userid):
    """Update user's username"""
    conn = await Database.get_connection()
    await conn.execute('UPDATE stats SET username = ? WHERE userid = ?', (username, userid))
    await conn.commit()

async def add_action(user, action):
    """Add user action to log"""
    conn = await Database.get_connection()
    await conn.execute('INSERT INTO actions (userid, action) VALUES (?, ?)', (user, action))
    await conn.commit()

async def execute_query(query, params=()):
    """Execute a query and return results"""
    conn = await Database.get_connection()
    cursor = await conn.execute(query, params)
    result = await cursor.fetchall()
    await conn.commit()
    return result

async def execute_query_one(query, params=()):
    """Execute a query and return single result"""
    conn = await Database.get_connection()
    cursor = await conn.execute(query, params)
    result = await cursor.fetchone()
    await conn.commit()
    return result

async def execute_update(query, params=()):
    """Execute an update query"""
    conn = await Database.get_connection()
    await conn.execute(query, params)
    await conn.commit()

# ===== FSM STATES =====
class Network_search(StatesGroup):
    id = State()

class Network_edit(StatesGroup):
    name = State()
    desc = State()

class Games(StatesGroup):
    game1_bet = State()
    game1_amount = State()
    game2_bet = State()
    game2_amount = State()

class Network_mailing(StatesGroup):
    text = State()

class Mailing(StatesGroup):
    user = State()
    text = State()

class Reowner(StatesGroup):
    userid = State()

class Send_channel(StatesGroup):
    url = State()
    text = State()
    
class Rename(StatesGroup):
    name = State()

# ===== BOT INITIALIZATION =====
bot = Bot(token=TOKEN)
dp = Dispatcher()

# Кулдаун для покупок ПК (1.5 секунды между покупками)
buy_cooldowns = {}
BUY_COOLDOWN = 1.5  # секунды

# ===== ROUTERS =====
fsm_router = Router()
callback_router = Router()
cmd_user_router = Router()
cmd_upgrades_router = Router()
cmd_games_router = Router()
cmd_franchise_router = Router()
cmd_economy_router = Router()
cmd_admin_router = Router()
cb_network_router = Router()
cb_economy_router = Router()
cb_donate_router = Router()
cb_games_router = Router()
cb_admin_router = Router()
# ===== KEYBOARDS =====
keyboard = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text='🖥 ПК в наличии'), KeyboardButton(text='👤 Профиль')],
        [KeyboardButton(text='🌐 Франшизы'), KeyboardButton(text='🛒 Магазин')],
        [KeyboardButton(text='🏆 Топ'), KeyboardButton(text='👑 Донат')]
    ],
    resize_keyboard=True,
    one_time_keyboard=False
)




@cmd_user_router.message(Command('upgrade_room_free'))
async def cmd_upgrade_room_free(message: Message):
    """Бесплатно повысить уровень комнаты на 1 (для всех пользователей)"""
    user = await execute_query_one('SELECT name, room FROM stats WHERE userid = ?', (message.from_user.id,))
    if not user:
        await message.answer('Сначала зарегистрируйтесь - /start')
        return
        
    await update_data(message.from_user.username, message.from_user.id)
    await add_action(message.from_user.id, 'cmd_upgrade_room_free')
    
    user_data = user
    current_room = user_data[1]
    new_room = current_room + 1
    
    # Получаем максимальный возможный уровень комнаты с учетом экспансии
    expansion_level = await get_expansion_level(message.from_user.id)
    max_room = 50 + (expansion_level * 10)  # 50 для базовой игры + 10 за каждую экспансию
    
    if new_room > max_room:
        await message.answer(
            f'❌ Вы достигли максимального уровня комнаты для вашей экспансии!\n\n'
            f'Текущий уровень: {current_room}\n'
            f'Максимум: {max_room}\n\n'
            f'Для дальнейшего роста выполните экспансию: /expansion'
        )
        return
    
    # Бесплатно повышаем уровень комнаты
    await execute_update(
        'UPDATE stats SET room = ? WHERE userid = ?',
        (new_room, message.from_user.id)
    )
    
    # Получаем название комнаты
    room_name = ROOM_NAMES.get(new_room, f"Комната уровня {new_room}")
    
    await message.answer(
        f'🎉 <b>Уровень комнаты повышен!</b>\n\n'
        f'🏠 Уровень: <b>{current_room} → {new_room}</b>\n'
        f'📝 Название: <b>{room_name}</b>\n'
        f'🖥️ Слоты: <b>{current_room * 5} → {new_room * 5}</b>\n\n'
        f'✨ Комната улучшена бесплатно!',
        parse_mode='HTML'
    )

# ===== FSM HANDLERS =====
@fsm_router.message(Network_search.id)
async def Network_id(message: Message, state: FSMContext):
    await state.clear()
    
    user = await execute_query_one('SELECT name FROM stats WHERE userid = ?', (message.from_user.id,))
    if not user:
        await message.answer('Сначала зарегистрируйтесь - /start')
        return
        
    await update_data(message.from_user.username, message.from_user.id)
    await add_action(message.from_user.id, 'Network_id')
    
    if message.text.isdigit():
        network = await execute_query('SELECT * FROM networks WHERE owner_id = ?', (int(message.text),))
    else:
        network = await execute_query('SELECT * FROM networks WHERE name = ?', (message.text,))
        
    if not network:
        await message.answer('❌ Ничего не найдено')
    else:
        network = network[0]
        markup = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text='↪️ Вступить', callback_data=f'network_join_{network[1]}_{message.from_user.id}')]
        ])
        status = ''
        if network[5] == 'open':
            status = 'Открытая'
        elif network[5] == 'close':
            status = 'Закрытая'
        elif network[5] == 'request':
            status = 'По заявке'
        await message.answer(f'🌐 Франшиза найдена!\nНазвание: {network[0]}\nОписание: {network[2]}\nСтатус: {status}', reply_markup=markup)

@fsm_router.message(Reowner.userid)
async def Reowner_userid(message: Message, state: FSMContext):
    await state.clear()
    
    user = await execute_query_one('SELECT name FROM stats WHERE userid = ?', (message.from_user.id,))
    if not user:
        await message.answer('Сначала зарегистрируйтесь - /start')
        return
        
    await update_data(message.from_user.username, message.from_user.id)
    await add_action(message.from_user.id, 'Reowner_userid')
    
    if message.text.isdigit():
        foundUser = await execute_query('SELECT userid FROM stats WHERE network = ? AND userid = ?', 
                                 (message.from_user.id, int(message.text)))
        if foundUser:
            await message.answer('🔄️ Вы успешно передали все права на франшизу')
            
            # Remove from admins
            admins_result = await execute_query('SELECT admins FROM networks WHERE owner_id = ?', 
                                              (message.from_user.id,))
            if admins_result:
                admins = parse_array(admins_result[0][0])
                if int(message.text) in admins:
                    admins.remove(int(message.text))
                    await execute_update('UPDATE networks SET admins = ? WHERE owner_id = ?', 
                                 (format_array(admins), message.from_user.id))
            
            # Transfer ownership
            await execute_update('UPDATE networks SET owner_id = ? WHERE owner_id = ?', 
                         (int(message.text), message.from_user.id))
            await execute_update('UPDATE stats SET network = ? WHERE network = ?', 
                         (int(message.text), message.from_user.id))
        else:
            await message.answer('❌ Такой пользователь не найден в вашей франшизе')
    else:
        await message.answer('⚠️ Введите корректный ID')

@fsm_router.message(Network_mailing.text)
async def Network_mailing_text(message: Message, state: FSMContext):
    await state.clear()
    
    user = await execute_query_one('SELECT name, network FROM stats WHERE userid = ?', (message.from_user.id,))
    if not user:
        await message.answer('Сначала зарегистрируйтесь - /start')
        return
        
    await update_data(message.from_user.username, message.from_user.id)
    await add_action(message.from_user.id, 'Network_mailing_text')
    
    user_data = user
    members = await execute_query('SELECT userid FROM stats WHERE network = ?', (user_data[1],))
    for member in members:
        try:
            if member[0] != message.from_user.id:
                await bot.send_message(member[0], f'📥 Вам пришла рассылка от владельца франшизы: {message.text}')
        except Exception:
            pass
            
    await execute_update('UPDATE networks SET mailing = ? WHERE owner_id = ?', 
                 (datetime.datetime.now(), message.from_user.id))
    await message.answer('📥 Рассылка успешно отправлена всем участникам франшизы')

@fsm_router.message(Network_edit.name)
async def Network_name(message: Message, state: FSMContext):
    if len(message.text) <= 50:
        await state.clear()
        
        user = await execute_query_one('SELECT name, network FROM stats WHERE userid = ?', (message.from_user.id,))
        if not user:
            await message.answer('Сначала зарегистрируйтесь - /start')
            return
            
        await update_data(message.from_user.username, message.from_user.id)
        await add_action(message.from_user.id, 'Network_name')
        
        user_data = user
        
        # Убираем проверку на символы - разрешаем любые символы и эмодзи
        name = await execute_query('SELECT * FROM networks WHERE name = ?', (message.text,))
        if not name:
            await execute_update('UPDATE networks SET name = ? WHERE owner_id = ?', (message.text, user_data[1]))
            markup = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text='🔙 Назад', callback_data=f'network_{message.from_user.id}')]
            ])
            await message.answer('✅ Вы успешно изменили название франшизы', reply_markup=markup)
        else:
            await message.answer('❌ Это название уже занято')
    else:
        await message.answer('❌ Название слишком длинное')

@fsm_router.message(Network_edit.desc)
async def Network_desc(message: Message, state: FSMContext):
    if len(message.text) <= 500:
        await state.clear()
        
        user = await execute_query_one('SELECT name, network FROM stats WHERE userid = ?', (message.from_user.id,))
        if not user:
            await message.answer('Сначала зарегистрируйтесь - /start')
            return
            
        await update_data(message.from_user.username, message.from_user.id)
        await add_action(message.from_user.id, 'Network_desc')
        
        user_data = user
        await execute_update('UPDATE networks SET description = ? WHERE owner_id = ?', (message.text, user_data[1]))
        markup = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text='🔙 Назад', callback_data=f'network_{message.from_user.id}')]
        ])
        await message.answer('✅ Вы успешно изменили описание франшизы', reply_markup=markup)
    else:
        await message.answer('❌ Описание слишком длинное')

@fsm_router.message(Games.game1_bet)
async def Game1_bet(message: Message, state: FSMContext):
    user = await execute_query_one('SELECT name FROM stats WHERE userid = ?', (message.from_user.id,))
    if not user:
        await message.answer('Сначала зарегистрируйтесь - /start')
        return
        
    await update_data(message.from_user.username, message.from_user.id)
    await add_action(message.from_user.id, 'Game1_bet')
    
    if message.text.lower() in ["орел", 'решка', 'орёл']:
        await state.update_data(bet=message.text.lower().replace('ё', 'е'))
        await state.set_state(Games.game1_amount)
        await message.answer('❓ Сколько вы хотите поставить денег?\nВведите целое число (минимум 5000) или /cancel для отмены действия')
    else:
        await message.answer('⚠️ Ставкой может быть только орел или решка')

@fsm_router.message(Games.game1_amount)
async def Game1_amount(message: Message, state: FSMContext):
    await state.clear()
    
    user = await execute_query_one('SELECT name, bal FROM stats WHERE userid = ?', (message.from_user.id,))
    if not user:
        await message.answer('Сначала зарегистрируйтесь - /start')
        return
        
    await update_data(message.from_user.username, message.from_user.id)
    await add_action(message.from_user.id, 'Game1_amount')
    
    user_data = user
    if message.text.isdigit():
        if int(message.text) >= 5000:
            if int(message.text) <= user_data[1]:
                value = random.randint(1, 100)
                if value <= 49:
                    await execute_update('UPDATE stats SET bal = bal + ? WHERE userid = ?', (int(message.text), message.from_user.id))
                    await message.answer(f'🎊 Вы угадали и получаете {int(message.text)*2}$')
                else:
                    await execute_update('UPDATE stats SET bal = bal - ? WHERE userid = ?', (int(message.text), message.from_user.id))
                    await message.answer(f'💥 Вы не угадали и теряете {message.text}$')
            else:
                await message.answer('❌ У вас не хватает $')
        else:
            await message.answer('❌ Минимальная ставка 5000$')
    else:
        await message.answer('⚠️ Можно использовать только целые числа')

@fsm_router.message(Games.game2_bet)
async def Game2_bet(message: Message, state: FSMContext):
    user = await execute_query_one('SELECT name FROM stats WHERE userid = ?', (message.from_user.id,))
    if not user:
        await message.answer('Сначала зарегистрируйтесь - /start')
        return
        
    await update_data(message.from_user.username, message.from_user.id)
    await add_action(message.from_user.id, 'Game2_bet')
    
    if message.text.isdigit() and int(message.text) in [1, 2, 3, 4, 5, 6]:
        await state.update_data(bet=int(message.text))
        await state.set_state(Games.game2_amount)
        await message.answer('❓ Сколько вы хотите поставить денег?\nВведите целое число (минимум 5000) или /cancel для отмены действия')
    else:
        await message.answer('⚠️ Ставкой может быть только число от 1 до 6')

@fsm_router.message(Games.game2_amount)
async def Game2_amount(message: Message, state: FSMContext):
    user = await execute_query_one('SELECT name, bal FROM stats WHERE userid = ?', (message.from_user.id,))
    if not user:
        await message.answer('Сначала зарегистрируйтесь - /start')
        return
        
    await update_data(message.from_user.username, message.from_user.id)
    await add_action(message.from_user.id, 'Game2_amount')
    
    user_data = user
    if message.text.isdigit():
        if int(message.text) >= 5000:
            if int(message.text) <= user_data[1]:
                sent_dice = await message.answer_dice(emoji='🎲')
                await asyncio.sleep(3)
                data = await state.get_data()
                if sent_dice.dice.value == data.get('bet'):
                    await execute_update('UPDATE stats SET bal = bal + ? WHERE userid = ?', (int(message.text)*5, message.from_user.id))
                    await message.answer(f'🎊 Вы угадали и получаете {int(message.text)*6}$')
                else:
                    await execute_update('UPDATE stats SET bal = bal - ? WHERE userid = ?', (int(message.text), message.from_user.id))
                    await message.answer(f'💥 Вы не угадали и теряете {message.text}$')
            else:
                await message.answer('❌ У вас не хватает $')
        else:
            await message.answer('❌ Минимальная ставка 5000$')
    else:
        await message.answer('⚠️ Можно использовать только целые числа')
    
    await state.clear()

@fsm_router.message(Mailing.user)
async def Mailing_user(message: Message, state: FSMContext):
    user = await execute_query_one('SELECT name FROM stats WHERE userid = ?', (message.from_user.id,))
    if not user:
        await message.answer('Сначала зарегистрируйтесь - /start')
        return
        
    await update_data(message.from_user.username, message.from_user.id)
    await add_action(message.from_user.id, 'Mailing_user')
    
    if not message.text.isdigit():
        await message.answer('⚠️ В айди могут быть только цифры')
        return
        
    user_target = await execute_query_one('SELECT userid FROM stats WHERE userid = ?', (int(message.text),))
    if user_target:
        await state.update_data(user=int(message.text))
        await message.answer('✍️ Теперь введите текст сообщения')
        await state.set_state(Mailing.text)
    else:
        await message.answer('❌ Такой пользователь не найден')

@fsm_router.message(Send_channel.url)
async def Send_channel_url(message: Message, state: FSMContext):
    await state.update_data(url=message.text)
    await bot.send_message(message.from_user.id, 'Укажите текст\nВведите /cancel для отмены действия')
    await state.set_state(Send_channel.text)

@fsm_router.message(Send_channel.text)
async def Send_channel_text(message: Message, state: FSMContext):
    data = await state.get_data()
    url = data.get('url')
    text = message.text.replace('_', '\\_')
    text = text.replace('Подробнее об обновлении', f'[Подробнее об обновлении]({url})')
    await bot.send_message(PCCLUB, text, disable_web_page_preview=True, parse_mode='Markdown')
    await state.clear()




# ===== ФУНКЦИИ ДЛЯ ЭКСПАНСИЙ =====

async def get_expansion_level(user_id: int) -> int:
    """Получить уровень экспансии пользователя"""
    result = await execute_query_one(
        'SELECT expansion_level FROM stats WHERE userid = ?',
        (user_id,)
    )
    return result[0] if result else 0

async def get_expansion_bonus(user_id: int) -> float:
    """Получить бонус экспансии в процентах"""
    expansion_level = await get_expansion_level(user_id)
    return expansion_level * 0.10  # +10% за каждую экспансию

async def can_do_expansion(user_id: int) -> bool:
    """Проверить, может ли пользователь сделать экспансию"""
    expansion_level = await get_expansion_level(user_id)
    
    if expansion_level >= 10:  # Максимум 10 экспансий
        return False
    
    # Проверяем, достиг ли пользователь максимума для текущей экспансии
    user_stats = await execute_query_one(
        'SELECT room FROM stats WHERE userid = ?',
        (user_id,)
    )
    
    if not user_stats:
        return False
    
    current_room = user_stats[0]
    required_room = 50 + (expansion_level * 10)  # 50 для 1 экспансии, 60 для 2 и т.д.
    
    return current_room >= required_room

async def do_expansion(user_id: int) -> bool:
    """Выполнить экспансию для пользователя"""
    if not await can_do_expansion(user_id):
        return False
    
    try:
        expansion_level = await get_expansion_level(user_id)
        new_expansion_level = expansion_level + 1
        
        # Обновляем уровень экспансии
        await execute_update(
            'UPDATE stats SET expansion_level = ? WHERE userid = ?',
            (new_expansion_level, user_id)
        )
        
        # Сбрасываем прогресс пользователя (вайп): баланс 5000$, комната 1, компьютеры 0
        # Сбрасываем улучшения, налоги, доход
        await execute_update(
            '''UPDATE stats SET 
               room = 1, 
               pc = 0, 
               income = 0, 
               taxes = 0, 
               bal = 5000,
               upgrade_internet = 0,
               upgrade_devices = 0,
               upgrade_interior = 0,
               upgrade_minibar = 0,
               upgrade_service = 0
               WHERE userid = ?''',
            (user_id,)
        )
        
        # Сбрасываем репутацию пользователя
        await execute_update(
            '''UPDATE user_reputation SET 
               reputation_points = 0,
               reputation_level = 1,
               total_earned_reputation = 0
               WHERE user_id = ?''',
            (user_id,)
        )
        
        # Удаляем все компьютеры пользователя
        await execute_update(
            'DELETE FROM pc WHERE userid = ?',
            (user_id,)
        )
        
        logger.info(f"User {user_id} completed expansion to level {new_expansion_level}")
        return True
        
    except Exception as e:
        logger.error(f"Error doing expansion for user {user_id}: {e}")
        return False

def get_expansion_stage_name(expansion_level: int) -> str:
    """Получить название этапа экспансии"""
    if expansion_level < 0 or expansion_level >= len(EXPANSION_STAGES):
        return "Неизвестный этап"
    return EXPANSION_STAGES[expansion_level]

def get_prices_for_expansion(expansion_level: int):
    """Получить цены ПК для текущей экспансии"""
    if expansion_level == 0:
        return prices  # Базовые ПК
    
    start_index = (expansion_level - 1) * 10
    end_index = start_index + 10
    
    if start_index >= len(prices_expansion):
        return []
    
    return prices_expansion[start_index:end_index]

def get_update_for_expansion(expansion_level: int):
    """Получить обновления комнаты для текущей экспансии"""
    if expansion_level == 0:
        return update  # Базовые обновления
    
    start_index = (expansion_level - 1) * 10
    end_index = start_index + 10
    
    if start_index >= len(update_expansion):
        return []
    
    return update_expansion[start_index:end_index]

def get_taxes_for_expansion(expansion_level: int):
    """Получить налоги для текущей экспансии"""
    if expansion_level == 0:
        return taxes  # Базовые налоги
    
    start_index = expansion_level * 10
    end_index = start_index + 10
    
    if start_index >= len(taxes_expansion):
        return []
    
    return taxes_expansion[start_index:end_index]

# ===== КОМАНДЫ ДЛЯ ЭКСПАНСИЙ =====

@cmd_user_router.message(Command('expansion'))
async def cmd_expansion(message: Message):
    """Показать информацию об экспансии"""
    user = await execute_query_one('SELECT name FROM stats WHERE userid = ?', (message.from_user.id,))
    if not user:
        await message.answer('Сначала зарегистрируйтесь - /start')
        return
        
    await update_data(message.from_user.username, message.from_user.id)
    await add_action(message.from_user.id, 'cmd_expansion')
    
    expansion_level = await get_expansion_level(message.from_user.id)
    expansion_bonus = await get_expansion_bonus(message.from_user.id)
    can_expand = await can_do_expansion(message.from_user.id)
    
    user_stats = await execute_query_one(
        'SELECT room FROM stats WHERE userid = ?',
        (message.from_user.id,)
    )
    
    current_slots = user_stats[0] if user_stats else 0
    required_slots = 50 + (expansion_level * 10)
    
    text = (
        f"🖥 <b>Экспансия:</b>\n\n"
        f"🆙 Ваш уровень экспансии: <b>{expansion_level}/10</b>\n"
        f"Этап: <b>{get_expansion_stage_name(expansion_level)}</b>\n"
        f"🔥 Ваш бонус: <b>+{expansion_bonus * 100:.1f}%</b>\n\n"
        f"Следующая экспансия: <b>{required_slots}</b> слотов\n"
        f"Ваши слоты сейчас: <b>{current_slots}</b>"
    )
    
    if can_expand:
        text += "\n\n🖥 <b>Вам доступна экспансия!</b>\n\n"
        text += "За каждую экспансию Вы получаете +10% к доходу всех ПК🔥\n\n"
        text += "Подтвердить экспансию - /expansion_confirm"
    
    await message.answer(text, parse_mode='HTML')

@cmd_user_router.message(Command('expansion_confirm'))
async def cmd_expansion_confirm(message: Message):
    """Подтвердить экспансию"""
    user = await execute_query_one('SELECT name FROM stats WHERE userid = ?', (message.from_user.id,))
    if not user:
        await message.answer('Сначала зарегистрируйтесь - /start')
        return
        
    await update_data(message.from_user.username, message.from_user.id)
    await add_action(message.from_user.id, 'cmd_expansion_confirm')
    
    if not await can_do_expansion(message.from_user.id):
        await message.answer('❌ Вам пока не доступна экспансия! Проверьте /expansion')
        return
    
    success = await do_expansion(message.from_user.id)
    
    if success:
        expansion_level = await get_expansion_level(message.from_user.id)
        expansion_bonus = await get_expansion_bonus(message.from_user.id)
        
        text = (
            f"🔥 <b>Поздравляем! Вы успешно сделали экспансию.</b>\n\n"
            f"Текущий этап:\n<b>{get_expansion_stage_name(expansion_level)}</b>\n"
            f"Экспансия: <b>{expansion_level}/10</b>\n\n"
            f"🎁 Бонус: +10% к доходу ПК\n"
            f"💰 Общий бонус: +{expansion_bonus * 100:.1f}%"
        )
        await message.answer(text, parse_mode='HTML')
    else:
        await message.answer('❌ Ошибка при выполнении экспансии!')

@fsm_router.message(Rename.name)
async def Rename_name(message: Message, state: FSMContext):
    await state.clear()
    
    user = await execute_query_one('SELECT name, premium FROM stats WHERE userid = ?', (message.from_user.id,))
    if not user:
        await message.answer('Сначала зарегистрируйтесь - /start')
        return
        
    await update_data(message.from_user.username, message.from_user.id)
    await add_action(message.from_user.id, 'Rename_name')
    
    user_data = user
    premium_date = datetime.datetime.strptime(user_data[1], '%Y-%m-%d %H:%M:%S') if isinstance(user_data[1], str) else user_data[1]
    
    if premium_date < datetime.datetime.now():
        if len(message.text) <= 15:
            if bool(re.fullmatch(r"[а-яА-Яa-zA-Z0-9 '\"]+", message.text)):
                name = await execute_query('SELECT * FROM stats WHERE name = ?', (message.text,))
                if not name:
                    await execute_update('UPDATE stats SET name = ? WHERE userid = ?', (message.text, message.from_user.id))
                    await message.answer('✅ Вы успешно изменили никнейм')
                else: 
                    await message.answer('⚠️ Этот никнейм уже занят')
            else:
                await message.answer('⚠️ Без PREMIUM можно использовать только русские и английские буквы, а так же цифры')
        else:
            await message.answer('❌ Никнейм слишком длинный, максимальная длинна никнейма 15 символов')
    else:
        if len(message.text) <= 30:
            name = await execute_query('SELECT * FROM stats WHERE name = ?', (message.text,))
            if not name:
                await execute_update('UPDATE stats SET name = ? WHERE userid = ?', (message.text, message.from_user.id))
                await message.answer('✅ Вы успешно изменили никнейм')
            else:
                await message.answer('⚠️ Этот никнейм уже занят')
        else:
            await message.answer('❌ Никнейм слишком длинный, максимальная длинна никнейма 30 символов')

@callback_router.callback_query(F.data.startswith('cancel'))
async def cb_cancel(callback: CallbackQuery):
    userid = callback.data.split('_')[-1]
    user = await execute_query_one('SELECT userid FROM stats WHERE userid = ?', (callback.from_user.id,))
    
    if not user or user[0] != int(userid):
        await callback.answer('⚠️ Это не твое сообщение', show_alert=True)
        return
        
    await update_data(callback.from_user.username, callback.from_user.id)
    await add_action(callback.from_user.id, 'cb_cancel')
    await callback.message.edit_text('❌ Действие отменено')

@callback_router.callback_query(F.data.startswith('success'))
async def cb_success(callback: CallbackQuery):
    userid = callback.data.split('_')[-1]
    user = await execute_query_one('SELECT userid FROM stats WHERE userid = ?', (callback.from_user.id,))
    
    if not user or user[0] != int(userid):
        await callback.answer('⚠️ Это не твое сообщение', show_alert=True)
        return
        
    await update_data(callback.from_user.username, callback.from_user.id)
    await add_action(callback.from_user.id, 'cb_success')
    
    labels = await execute_query('SELECT label FROM orders WHERE userid = ? AND success = 0', 
                          (callback.from_user.id,))
    
    # Simplified payment verification (replace with actual YooMoney API)
    success = False
    for label in labels:
        # Mock payment verification - replace with actual YooMoney API call
        if random.random() > 0.5:  # 50% chance of success for demo
            success = True
            successful_label = label[0]
            break
    
    if success:
        title = await execute_query('SELECT users FROM titles WHERE id = ?', ('first_donate',))
        stats = await execute_query('SELECT premium, ref FROM stats WHERE userid = ?', (callback.from_user.id,))
        order = await execute_query('SELECT days FROM orders WHERE label = ?', (successful_label,))
        days = order[0][0] if order else 1
        
        if title and callback.from_user.id not in parse_array(title[0][0]):
            new_users = parse_array(title[0][0])
            new_users.append(callback.from_user.id)
            await execute_update('UPDATE titles SET users = ? WHERE id = ?', (format_array(new_users), 'first_donate'))
        
        stats_data = stats[0]
        # ИСПРАВЛЕННАЯ ЧАСТЬ - безопасное преобразование даты
        premium_date = safe_parse_datetime(stats_data[0])
        if premium_date and premium_date > datetime.datetime.now():
            new_premium = premium_date + datetime.timedelta(days=days)
        else:
            new_premium = datetime.datetime.now() + datetime.timedelta(days=days)
        
        await execute_update('UPDATE stats SET premium = ? WHERE userid = ?', (new_premium, callback.from_user.id))
        await execute_update('UPDATE orders SET success = 1 WHERE label = ?', (successful_label,))
        
        if stats_data[1]:
            ref_premium = await execute_query('SELECT premium FROM stats WHERE userid = ?', (stats_data[1],))
            if ref_premium:
                ref_premium_date = safe_parse_datetime(ref_premium[0][0])
                if ref_premium_date and ref_premium_date > datetime.datetime.now():
                    new_ref_premium = ref_premium_date + datetime.timedelta(days=days/4)
                else:
                    new_ref_premium = datetime.datetime.now() + datetime.timedelta(days=days/4)
                await execute_update('UPDATE stats SET premium = ? WHERE userid = ?', (new_ref_premium, stats_data[1]))
        
        await callback.message.edit_text('✅ Оплата прошла успешно. Премиум зачислен на твой аккаунт!')
    else:
        await callback.message.edit_text('❌ Не оплачено')

# ===== COMMAND HANDLERS =====
async def get_work_stats(user_id: int):
    """Получить статы работы"""
    result = await execute_query_one('SELECT exp, last_work FROM user_work_stats WHERE user_id = ?', (user_id,))
    if result:
        last_work = datetime.datetime.fromisoformat(result[1]) if result[1] else None
        return result[0], last_work
    await execute_update('INSERT OR IGNORE INTO user_work_stats (user_id) VALUES (?)', (user_id,))
    return 0, None

async def do_work(user_id: int, job_id: int):
    """Выполнить работу"""
    job = next((j for j in WORK_JOBS if j['id'] == job_id), None)
    if not job:
        return False, "Нет такой работы"

    exp, last_work = await get_work_stats(user_id)

    # Проверка только минимального опыта (убрано ограничение max_exp)
    if exp < job['min_exp']:
        return False, f"Нужно {job['min_exp']}+ опыта (у вас {exp})"

    if last_work:
        next_work = last_work + datetime.timedelta(hours=1)
        if datetime.datetime.now() < next_work:
            time_left = next_work - datetime.datetime.now()
            hours = time_left.seconds // 3600
            minutes = (time_left.seconds % 3600) // 60
            return False, f"⏳ Вы уже работали недавно!\nСледующая работа возможна через: {hours}ч {minutes}м"
    
    reward = job['reward']
    user = await execute_query_one('SELECT bal FROM stats WHERE userid = ?', (user_id,))
    if not user:
        return False, "Ошибка"
    
    # Добавляем репутацию за работу (уровень работы = количество очков)
    rep_points = job_id
    new_points, new_level, level_up = await add_reputation(user_id, rep_points, "work")
    
    new_bal = user[0] + reward
    
    await execute_update('UPDATE stats SET bal = ? WHERE userid = ?', (new_bal, user_id))
    await execute_update('''
        UPDATE user_work_stats 
        SET exp = exp + 1, last_work = ?, total_earned = total_earned + ?
        WHERE user_id = ?
    ''', (datetime.datetime.now().isoformat(), reward, user_id))
    
    # Новое сообщение с репутацией
    result_text = f"✅ {job['name']}\n💵 +{reward}$\n🌟 Опыт: {exp+1}\n✨ +{rep_points} Репутации"
    
    if level_up:
        rep_info = await get_current_reputation_info(user_id)
        result_text += f"\n\n🎉 Новый уровень репутации: {rep_info['level_name']}!"
    
    return True, result_text


@cmd_user_router.message(Command("work"))
async def work_list(message: Message):
    user_id = message.from_user.id
    exp, _ = await get_work_stats(user_id)

    text = "💼 Работы:\n"
    for job in WORK_JOBS:
        if job['min_exp'] <= exp:
            status = "✅"
            req = f"{job['min_exp']}+"
        else:
            status = "🔒"
            req = f"{job['min_exp']}+"

        text += f"{status} /work_{job['id']} - {job['name']}\n${job['reward']} | {req}\n\n"

    await message.answer(text)
    
@cmd_user_router.message(F.text.regexp(r'^/work_(\d+)(@\w+)?$'))
async def work_start(message: Message):
    # Проверяем бустер автоматизации
    user_boosters = await execute_query_one(
        'SELECT auto_booster_end FROM stats WHERE userid = ?',
        (message.from_user.id,)
    )
    
    if user_boosters and user_boosters[0]:
        auto_booster_end = safe_parse_datetime(user_boosters[0])
        if auto_booster_end and auto_booster_end > datetime.datetime.now():
            await message.answer(
                '⏰ <b>Работа выполняется автоматически!</b>\n\n'
                'У вас активен бустер автоматизации. Система автоматически выполняет работу за вас каждый час.\n\n'
                'Чтобы работать вручную, дождитесь окончания бустера.',
                parse_mode='HTML'
            )
            return
    
    user_id = message.from_user.id
    try:
        # Убираем @username если есть
        command_text = message.text.split('@')[0]
        job_id = int(command_text.split('_')[1])
        success, result = await do_work(user_id, job_id)
        await message.answer(result)
    except:
        await message.answer("❌ Ошибка")
        
@cmd_user_router.message(CommandStart())
async def cmd_start(message: Message):
    user = await execute_query_one('SELECT name FROM stats WHERE userid = ?', (message.from_user.id,))
    if not user:
        # Формируем полное имя из имени и фамилии
        full_name = message.from_user.first_name or ""
        if message.from_user.last_name:
            full_name += f" {message.from_user.last_name}"
        
        # Устанавливаем полное имя из Telegram при создании пользователя
        await execute_update('INSERT INTO stats (userid, username, name) VALUES (?, ?, ?)', 
                         (message.from_user.id, message.from_user.username, full_name.strip()))

        # Уведомляем админов о новом пользователе (без реферальной информации)
        for admin_id in ADMIN:
            try:
                await bot.send_message(
                    admin_id,
                    f'Новый пользователь: [{message.from_user.first_name}](tg://user?id={message.from_user.id}) @{message.from_user.username}',
                    parse_mode='Markdown'
                )
            except Exception as e:
                logger.error(f"Error sending notification to admin {admin_id}: {e}")
    
    await update_data(message.from_user.username, message.from_user.id)
    await add_action(message.from_user.id, 'cmd_start')
    
    # Получаем текущее имя пользователя из базы данных
    current_user = await execute_query_one('SELECT name FROM stats WHERE userid = ?', (message.from_user.id,))
    user_name = current_user[0] if current_user else message.from_user.first_name
    
    welcome_text = (
        f'👋 Привет, {user_name}!\n\n'
        'Добро пожаловать в мир компьютеров, где ты вступаешь на путь владельца собственного ПК клуба 🤩\n\n'
        '✨ Твоя цель: Построить самый крутой и прибыльный ПК-клуб!\n\n'
        'Что ждет тебя в симуляторе? 💰💻\n'
        'Постепенно расширяй свою комнату. Сделай её стильной и комфортной, благодаря закупка Мощного "Железа"!\n\n'
        'Чем круче ПК, тем больше доход и довольнее клиенты. 🚀\n\n'
        'А самое главное покупай рекламу! Маркетинг — Двигатель Прогресса🔥\n\n'
        '👉 А сейчас быстрее беги в Магазин (Команда: /shop) и купи свои первые компьютеры, чтобы твои кресла не пустовали, а касса начала работать!\n\n'
        'Желаем удачи в бизнесе! Пусть твой клуб станет №1! 🏆🎉'
    )
    
    if message.chat.id == message.from_user.id:
        await message.answer(welcome_text, reply_markup=keyboard)
    else:
        await message.answer(welcome_text)

        
@cmd_admin_router.message(Command('give_all_premium'))
async def cmd_give_all_premium(message: Message):
    if message.from_user.id not in ADMIN:
        await message.answer('❌ Недостаточно прав')
        return
        
    text_parts = message.text.split(' ')
    
    if len(text_parts) != 2 or not text_parts[1].isdigit():
        await message.answer('⚠️ Используйте: /give_all_premium (количество_дней)')
        return
        
    days = int(text_parts[1])
    
    if days <= 0:
        await message.answer('❌ Количество дней должно быть больше 0')
        return
        
    try:
        # Получаем общее количество пользователей
        total_users = await execute_query('SELECT COUNT(*) FROM stats')
        total_count = total_users[0][0] if total_users else 0
        
        if total_count == 0:
            await message.answer('❌ В базе нет пользователей')
            return
            
        # Выдаем премиум всем пользователям
        new_premium_date = datetime.datetime.now() + datetime.timedelta(days=days)
        
        result = await execute_update(
            'UPDATE stats SET premium = ?', 
            (new_premium_date,)
        )
        
        await message.answer(
            f'✅ <b>Премиум успешно выдан всем пользователям!</b>\n\n'
            f'👥 Количество пользователей: <b>{total_count}</b>\n'
            f'⏰ Срок: <b>{days}</b> дней\n'
            f'📅 Действует до: <code>{new_premium_date.strftime("%d.%m.%Y %H:%M")}</code>',
            parse_mode='HTML'
        )
        
        logger.info(f"Admin {message.from_user.id} gave premium to all users for {days} days")

    except Exception as e:
        logger.error(f"Error giving premium to all users: {e}")
        await message.answer('❌ Ошибка при выдаче премиума')


@cmd_admin_router.message(Command('give_all_boost'))
async def cmd_give_all_boost(message: Message):
    if message.from_user.id not in ADMIN:
        await message.answer('❌ Недостаточно прав')
        return

    text_parts = message.text.split(' ')

    if len(text_parts) != 2 or not text_parts[1].isdigit():
        await message.answer('⚠️ Используйте: /give_all_boost (количество_дней)')
        return

    days = int(text_parts[1])

    if days <= 0:
        await message.answer('❌ Количество дней должно быть больше 0')
        return

    try:
        total_users = await execute_query('SELECT COUNT(*) FROM stats')
        total_count = total_users[0][0] if total_users else 0

        if total_count == 0:
            await message.answer('❌ В базе нет пользователей')
            return

        new_booster_date = datetime.datetime.now() + datetime.timedelta(days=days)

        result = await execute_update(
            'UPDATE stats SET income_booster_end = ?',
            (new_booster_date,)
        )

        await message.answer(
            f'✅ <b>Бустер дохода выдан всем пользователям!</b>\n\n'
            f'👥 Количество пользователей: <b>{total_count}</b>\n'
            f'⏰ Срок: <b>{days}</b> дней\n'
            f'📅 Действует до: <code>{new_booster_date.strftime("%d.%m.%Y %H:%M")}</code>',
            parse_mode='HTML'
        )

        logger.info(f"Admin {message.from_user.id} gave income booster to all users for {days} days")

    except Exception as e:
        logger.error(f"Error giving income booster to all users: {e}")
        await message.answer('❌ Ошибка при выдаче бустера')


@cmd_admin_router.message(Command('give_all_auto'))
async def cmd_give_all_auto(message: Message):
    if message.from_user.id not in ADMIN:
        await message.answer('❌ Недостаточно прав')
        return

    text_parts = message.text.split(' ')

    if len(text_parts) != 2 or not text_parts[1].isdigit():
        await message.answer('⚠️ Используйте: /give_all_auto (количество_дней)')
        return

    days = int(text_parts[1])

    if days <= 0:
        await message.answer('❌ Количество дней должно быть больше 0')
        return

    try:
        total_users = await execute_query('SELECT COUNT(*) FROM stats')
        total_count = total_users[0][0] if total_users else 0

        if total_count == 0:
            await message.answer('❌ В базе нет пользователей')
            return

        new_auto_date = datetime.datetime.now() + datetime.timedelta(days=days)

        result = await execute_update(
            'UPDATE stats SET auto_booster_end = ?',
            (new_auto_date,)
        )

        await message.answer(
            f'✅ <b>Авторабота и автоналог выданы всем пользователям!</b>\n\n'
            f'👥 Количество пользователей: <b>{total_count}</b>\n'
            f'⏰ Срок: <b>{days}</b> дней\n'
            f'📅 Действует до: <code>{new_auto_date.strftime("%d.%m.%Y %H:%M")}</code>',
            parse_mode='HTML'
        )

        logger.info(f"Admin {message.from_user.id} gave auto booster to all users for {days} days")

    except Exception as e:
        logger.error(f"Error giving auto booster to all users: {e}")
        await message.answer('❌ Ошибка при выдаче автобустера')


@cmd_admin_router.message(Command('add_rep'))
async def cmd_add_rep(message: Message):
    """Добавить репутацию пользователю"""
    if message.from_user.id not in ADMIN:
        await message.answer('❌ Недостаточно прав')
        return
        
    text_parts = message.text.split(' ')
    
    if len(text_parts) != 3 or not text_parts[1].isdigit() or not text_parts[2].isdigit():
        await message.answer(
            '⚠️ Используйте: /add_rep (ID_пользователя) (количество_репутации)\n\n'
            '*Пример:*\n'
            '`/add_rep 5929120983 1000`'
        )
        return
        
    target_user_id = int(text_parts[1])
    rep_amount = int(text_parts[2])
    
    if rep_amount <= 0:
        await message.answer('❌ Количество репутации должно быть больше 0')
        return
        
    try:
        # Проверяем существование пользователя
        user = await execute_query_one(
            'SELECT name FROM stats WHERE userid = ?', 
            (target_user_id,)
        )
        
        if not user:
            await message.answer('❌ Пользователь не найден')
            return
            
        user_name = user[0]
        
        # Добавляем репутацию
        new_points, new_level, level_up = await add_reputation(
            target_user_id, rep_amount, "admin_command"
        )
        
        # Получаем информацию о новом уровне
        rep_info = await get_current_reputation_info(target_user_id)
        
        response_text = (
            f'✅ <b>Репутация добавлена!</b>\n\n'
            f'👤 Пользователь: <b>{user_name}</b>\n'
            f'🆔 ID: <code>{target_user_id}</code>\n'
            f'✨ Добавлено: <b>{rep_amount}</b> очков репутации\n'
            f'📊 Теперь: <b>{new_points}</b> очков\n'
            f'🏆 Уровень: <b>{rep_info["level_name"]}</b>'
        )
        
        if level_up:
            response_text += f'\n\n🎉 <b>Пользователь достиг нового уровня репутации!</b>'
        
        await message.answer(response_text, parse_mode='HTML')
        
        # Уведомляем пользователя
        try:
            user_notification = (
                f'🎉 <b>Вам добавлена репутация!</b>\n\n'
                f'✨ +{rep_amount} очков репутации\n'
                f'📊 Теперь у вас: {new_points} очков\n'
                f'🏆 Уровень: {rep_info["level_name"]}'
            )
            
            if level_up:
                user_notification += f'\n\n🎊 <b>Поздравляем с новым уровнем репутации!</b>'
            
            await bot.send_message(target_user_id, user_notification, parse_mode='HTML')
        except Exception as e:
            logger.warning(f"Could not notify user {target_user_id}: {e}")
        
        logger.info(f"Admin {message.from_user.id} added {rep_amount} reputation to user {target_user_id}")
        
    except Exception as e:
        logger.error(f"Error adding reputation: {e}")
        await message.answer('❌ Ошибка при добавлении репутации')

@cmd_admin_router.message(Command('set_bal'))
async def cmd_set_bal(message: Message):
    """Установить баланс пользователю"""
    if message.from_user.id not in ADMIN:
        await message.answer('❌ Недостаточно прав')
        return

    text_parts = message.text.split(' ')

    if len(text_parts) != 3:
        await message.answer(
            '⚠️ Используйте: /set_bal (ID_пользователя) (сумма)\n\n'
            '*Пример:*\n'
            '`/set_bal 5929120983 1000000`\n\n'
            '*Для себя можно использовать:*\n'
            '`/set_bal me 1000000`'
        )
        return

    # Проверка корректности ID
    if text_parts[1].lower() != 'me' and not text_parts[1].isdigit():
        await message.answer('❌ ID пользователя должен быть числом или "me"')
        return

    # Определяем целевого пользователя
    if text_parts[1].lower() == 'me':
        target_user_id = message.from_user.id
    else:
        target_user_id = int(text_parts[1])

    # Парсим сумму
    try:
        amount = Decimal(text_parts[2])
        if amount < 0:
            await message.answer('❌ Сумма не может быть отрицательной')
            return
    except:
        await message.answer('❌ Неверный формат суммы')
        return

    try:
        # Проверяем существование пользователя
        user = await execute_query_one(
            'SELECT name FROM stats WHERE userid = ?',
            (target_user_id,)
        )

        if not user:
            await message.answer('❌ Пользователь не найден')
            return

        user_name = user[0]

        # Устанавливаем баланс
        await execute_update(
            'UPDATE stats SET bal = ? WHERE userid = ?',
            (str(amount), target_user_id)
        )

        response_text = (
            f'✅ <b>Баланс установлен!</b>\n\n'
            f'👤 Пользователь: <b>{user_name}</b>\n'
            f'🆔 ID: <code>{target_user_id}</code>\n'
            f'💰 Новый баланс: <b>{format_number_short(amount, True)}$</b>'
        )

        await message.answer(response_text, parse_mode='HTML')

        # Уведомляем пользователя (если это не админ сам себе)
        if target_user_id != message.from_user.id:
            try:
                user_notification = (
                    f'💰 <b>Ваш баланс был изменен администратором!</b>\n\n'
                    f'💳 Новый баланс: <b>{format_number_short(amount, True)}$</b>'
                )

                await bot.send_message(target_user_id, user_notification, parse_mode='HTML')
            except Exception as e:
                logger.warning(f"Could not notify user {target_user_id}: {e}")

        logger.info(f"Admin {message.from_user.id} set balance {amount}$ to user {target_user_id}")

    except Exception as e:
        logger.error(f"Error setting balance: {e}")
        await message.answer('❌ Ошибка при установке баланса')

@cmd_admin_router.message(Command('give_premium'))
async def cmd_give_premium(message: Message):
    if message.from_user.id not in ADMIN:
        await message.answer('❌ Недостаточно прав')
        return
        
    text_parts = message.text.split(' ')
    
    if len(text_parts) != 3 or not text_parts[1].isdigit() or not text_parts[2].isdigit():
        await message.answer('⚠️ Используйте: /give_premium (ID_пользователя) (количество_дней)')
        return
        
    target_user_id = int(text_parts[1])
    days = int(text_parts[2])
    
    if days <= 0:
        await message.answer('❌ Количество дней должно быть больше 0')
        return
        
    try:
        # Проверяем существование пользователя
        user = await execute_query_one(
            'SELECT name, premium FROM stats WHERE userid = ?', 
            (target_user_id,)
        )
        
        if not user:
            await message.answer('❌ Пользователь не найден')
            return
            
        user_name = user[0]
        current_premium = user[1]
        
        # Рассчитываем новую дату премиума
        new_premium_date = datetime.datetime.now() + datetime.timedelta(days=days)
        
        # Если у пользователя уже есть активный премиум, продлеваем его
        if current_premium:
            current_premium_date = safe_parse_datetime(current_premium)
            if current_premium_date and current_premium_date > datetime.datetime.now():
                new_premium_date = current_premium_date + datetime.timedelta(days=days)
        
        # Выдаем/продлеваем премиум
        await execute_update(
            'UPDATE stats SET premium = ? WHERE userid = ?', 
            (new_premium_date, target_user_id)
        )
        
        # Отправляем уведомление пользователю
        try:
            await bot.send_message(
                target_user_id,
                f'🎉 <b>Вам выдан PREMIUM!</b>\n\n'
                f'⏰ Срок: <b>{days}</b> дней\n'
                f'📅 Действует до: <code>{new_premium_date.strftime("%d.%m.%Y %H:%M")}</code>\n\n'
                f'✨ Теперь вы получаете +50% к доходу!',
                parse_mode='HTML'
            )
        except Exception as e:
            logger.warning(f"Could not notify user {target_user_id}: {e}")
        
        await message.answer(
            f'✅ <b>Премиум успешно выдан!</b>\n\n'
            f'👤 Пользователь: <b>{user_name}</b>\n'
            f'🆔 ID: <code>{target_user_id}</code>\n'
            f'⏰ Срок: <b>{days}</b> дней\n'
            f'📅 Действует до: <code>{new_premium_date.strftime("%d.%m.%Y %H:%M")}</code>',
            parse_mode='HTML'
        )
        
        logger.info(f"Admin {message.from_user.id} gave premium to user {target_user_id} for {days} days")
        
    except Exception as e:
        logger.error(f"Error giving premium to user: {e}")
        await message.answer('❌ Ошибка при выдаче премиума')                                                
                                
        
@cmd_user_router.message(Command('nickname'))
async def cmd_nickname(message: Message):
    user = await execute_query_one('SELECT name FROM stats WHERE userid = ?', (message.from_user.id,))
    if not user:
        await message.answer('Сначала зарегистрируйтесь - /start')
        return
        
    await update_data(message.from_user.username, message.from_user.id)
    await add_action(message.from_user.id, 'cmd_nickname')
    
    # Получаем текст после команды
    text_parts = message.text.split(' ', 1)
    if len(text_parts) < 2:
        await message.answer('❌ Используйте: /nickname (новый_никнейм)\nПример: /nickname Игрок123')
        return
    
    new_nickname = text_parts[1].strip()
    
    # Проверка длины
    if len(new_nickname) > 15:
        await message.answer('❌ Никнейм слишком длинный, максимальная длина никнейма 15 символов')
        return
    
    # Проверка на запрещенные символы и ссылки
    forbidden_patterns = [
        r'http://', r'https://', r't\.me/', r'@'
    ]
    
    for pattern in forbidden_patterns:
        if re.search(pattern, new_nickname, re.IGNORECASE):
            await message.answer('❌ В нике нельзя использовать ссылки (http://, https://, t.me/) и символ @')
            return
    
    # Проверка на существующий ник
    name = await execute_query('SELECT * FROM stats WHERE name = ?', (new_nickname,))
    if not name:
        await execute_update('UPDATE stats SET name = ? WHERE userid = ?', (new_nickname, message.from_user.id))
        await message.answer(f'✅ Вы успешно изменили никнейм на: {new_nickname}')
    else: 
        await message.answer('⚠️ Этот никнейм уже занят')

@cmd_user_router.message(Command('stats'))
async def cmd_stats(message: Message):
    user = await execute_query_one('SELECT name, all_wallet, reg_day, name, all_pcs, max_bal FROM stats WHERE userid = ?', 
                        (message.from_user.id,))
    if not user:
        await message.answer('Сначала зарегистрируйтесь - /start')
        return
        
    await update_data(message.from_user.username, message.from_user.id)
    await add_action(message.from_user.id, 'cmd_stats')
    
    user_data = user
    refs = await execute_query('SELECT COUNT(*) FROM stats WHERE ref = ?', (message.from_user.id,))
    
    # Получаем опыт работы
    work_exp, _ = await get_work_stats(message.from_user.id)
    
    reg_day = user_data[2]
    if isinstance(reg_day, str):
        reg_day = reg_day[:10]
    else:
        reg_day = reg_day.strftime('%Y-%m-%d') if hasattr(reg_day, 'strftime') else str(reg_day)[:10]
    
    # Получаем бонусы от улучшений
    upgrades = await execute_query(
        'SELECT upgrade_internet, upgrade_devices, upgrade_service FROM stats WHERE userid = ?',
        (message.from_user.id,)
    )
    
    total_upgrade_bonus = 0
    if upgrades:
        total_upgrade_bonus = sum(upgrades[0])
    
    # Проверяем PREMIUM статус для бонусов доната
    premium_bonus = 0
    premium = await execute_query_one('SELECT premium FROM stats WHERE userid = ?', (message.from_user.id,))
    if premium and premium[0]:
        premium_date = safe_parse_datetime(premium[0])
        if premium_date and premium_date > datetime.datetime.now():
            premium_bonus = 50
    
    # Получаем бонус экспансии
    expansion_bonus = await get_expansion_bonus(message.from_user.id)
    expansion_bonus_percent = expansion_bonus * 100
    
    await message.answer(
        f'📈 *Статистика {user_data[3]}*\n\n'
        f'🌟 Опыт работы: *{work_exp}*\n'
        f'🖥 Куплено ПК за всё время: *{user_data[4]}*\n'
        f'📅 Дата регистрации: *{reg_day}*\n'
        f'💫 Бонусы улучшений: *+{total_upgrade_bonus}%*\n'
        f'🔥 Бонусы от доната: *+{premium_bonus}%*\n'
        f'🎁 Бонусы за Экспансию: *+{expansion_bonus_percent:.1f}%*',
        parse_mode='Markdown'
    )

    
@cmd_user_router.message(Command('my_pcs'))
async def cmd_my_pcs(message: Message):
    user = await execute_query_one('SELECT name, room, pc FROM stats WHERE userid = ?', (message.from_user.id,))
    if not user:
        await message.answer('Сначала зарегистрируйтесь - /start')
        return
        
    await update_data(message.from_user.username, message.from_user.id)
    await add_action(message.from_user.id, 'cmd_my_pcs')
    
    user_data = user
    max_slots = user_data[1] * 5
    used_slots = user_data[2]
    
    text = f'🖥 Ваши компьютеры:\n📊 Слоты: {used_slots}/{max_slots}\n\n'
    
    # Получаем уровень экспансии пользователя
    expansion_level = await get_expansion_level(message.from_user.id)
    
    # Создаем полный список всех ПК (базовые + экспансии)
    all_prices = prices.copy()
    
    # Добавляем ПК из экспансий
    for expansion in range(1, expansion_level + 1):
        expansion_pcs = get_prices_for_expansion(expansion)
        all_prices.extend(expansion_pcs)
    
    # Получаем все компьютеры пользователя
    for price_data in all_prices:
        level = price_data[0]
        pcs = await execute_query('SELECT income FROM pc WHERE userid = ? AND lvl = ?', 
                           (message.from_user.id, level))
        total_income = 0
        total_pcs = len(pcs)
        
        if total_pcs > 0:
            for pc in pcs:
                total_income += Decimal(str(pc[0]))
            
            text += f'Компьютер {level} ур. {total_pcs} шт.\n'
            text += f'Доход: {format_number_short(total_income, True)}$.\n'
            text += f'Продать: /sell_{level}\n\n'
    
    text += 'Продать: /sell_(id) (кол-во)'
    await message.answer(text)
    

async def get_available_pcs(user_id: int):
    """Получить доступные ПК для пользователя с учетом экспансии"""
    expansion_level = await get_expansion_level(user_id)
    user_stats = await execute_query_one('SELECT room FROM stats WHERE userid = ?', (user_id,))
    
    if not user_stats:
        return []
    
    current_room = user_stats[0]
    available_pcs = []
    
    # Базовые ПК
    for pc in prices:
        if pc[0] <= current_room:
            available_pcs.append(pc)
    
    # ПК из экспансий
    for expansion in range(1, expansion_level + 1):
        expansion_pcs = get_prices_for_expansion(expansion)
        for pc in expansion_pcs:
            if pc[0] <= current_room:
                available_pcs.append(pc)
    
    return available_pcs

async def get_room_upgrades(user_id: int):
    """Получить доступные улучшения комнаты с учетом экспансии"""
    expansion_level = await get_expansion_level(user_id)
    user_stats = await execute_query_one('SELECT room FROM stats WHERE userid = ?', (user_id,))
    
    if not user_stats:
        return []
    
    current_room = user_stats[0]
    available_upgrades = []
    
    # Базовые улучшения
    for upgrade_data in update:
        if upgrade_data[0] > current_room:
            available_upgrades.append(upgrade_data)
    
    # Улучшения из экспансий
    for expansion in range(1, expansion_level + 1):
        expansion_upgrades = get_update_for_expansion(expansion)
        for upgrade_data in expansion_upgrades:
            if upgrade_data[0] > current_room:
                available_upgrades.append(upgrade_data)
    
    return available_upgrades
    
async def get_user_reputation(user_id: int):
    """Получить информацию о репутации пользователя"""
    result = await execute_query_one(
        'SELECT reputation_points, reputation_level, total_earned_reputation FROM user_reputation WHERE user_id = ?',
        (user_id,)
    )
    if result:
        return result
    # Создаем запись, если не существует
    await execute_update(
        'INSERT INTO user_reputation (user_id) VALUES (?)',
        (user_id,)
    )
    return (0, 1, 0)

async def add_reputation(user_id: int, points: int, reason: str = ""):
    """Добавить очки репутации пользователю"""
    current_points, current_level, total_earned = await get_user_reputation(user_id)
    new_points = current_points + points
    new_total_earned = total_earned + points
    
    # Обновляем очки
    await execute_update(
        'UPDATE user_reputation SET reputation_points = ?, total_earned_reputation = ? WHERE user_id = ?',
        (new_points, new_total_earned, user_id)
    )
    
    # Проверяем повышение уровня
    new_level = current_level
    for level_info in REPUTATION_LEVELS:
        if new_points >= level_info["points_required"] and level_info["level"] > new_level:
            new_level = level_info["level"]
    
    if new_level > current_level:
        await execute_update(
            'UPDATE user_reputation SET reputation_level = ? WHERE user_id = ?',
            (new_level, user_id)
        )
        return new_points, new_level, True  # Возвращаем с флагом повышения уровня
    
    return new_points, current_level, False

async def get_reputation_bonuses(user_id: int):
    """Получить бонусы от репутации"""
    _, level, _ = await get_user_reputation(user_id)
    for level_info in REPUTATION_LEVELS:
        if level_info["level"] == level:
            return level_info["income_bonus"], level_info["tax_reduction"]
    return 0.0, 0.0

async def get_current_reputation_info(user_id: int):
    """Получить текущую информацию о репутации для отображения"""
    points, level, _ = await get_user_reputation(user_id)
    current_level_info = REPUTATION_LEVELS[level - 1]
    next_level_info = REPUTATION_LEVELS[level] if level < len(REPUTATION_LEVELS) else None
    
    points_needed = next_level_info["points_required"] - points if next_level_info else 0
    
    return {
        "level": level,
        "level_name": current_level_info["name"],
        "points": points,
        "points_needed": points_needed,
        "income_bonus": current_level_info["income_bonus"] * 100,
        "tax_reduction": current_level_info["tax_reduction"] * 100,
        "next_level_name": next_level_info["name"] if next_level_info else "Максимум"
    }


@cmd_user_router.message(Command("reputation"))
async def cmd_rep(message: Message):
    user = await execute_query_one('SELECT name FROM stats WHERE userid = ?', (message.from_user.id,))
    if not user:
        await message.answer('Сначала зарегистрируйтесь - /start')
        return
        
    await update_data(message.from_user.username, message.from_user.id)
    await add_action(message.from_user.id, 'cmd_rep')
    
    rep_info = await get_current_reputation_info(message.from_user.id)
    
    text = (
        f"✨ Репутация:\n\n"
        f"Уровень престижа: {rep_info['level']}/{len(REPUTATION_LEVELS)} - {rep_info['level_name']}\n"
        f"Очки репутации: {rep_info['points']}"
    )
    
    if rep_info['points_needed'] > 0:
        text += f"/{rep_info['points'] + rep_info['points_needed']} ✨\n\n"
    else:
        text += " ✨ (Максимум)\n\n"
    
    text += (
        f"Ваш бонус от уровня репутации:\n"
        f"🖥 Компьютеры: +{rep_info['income_bonus']:.1f}% к доходу\n"
        f"💵 Налог: -{rep_info['tax_reduction']:.1f}% налога"
    )
    
    await message.answer(text)
    
    
    
@cmd_user_router.message(Command('my_ad'))
async def cmd_my_ad(message: Message):
    user = await execute_query_one('SELECT name FROM stats WHERE userid = ?', (message.from_user.id,))
    if not user:
        await message.answer('Сначала зарегистрируйтесь - /start')
        return
        
    await update_data(message.from_user.username, message.from_user.id)
    await add_action(message.from_user.id, 'cmd_my_ad')
    
    user_ad = await execute_query('SELECT * FROM ads WHERE userid = ? ORDER BY dt DESC LIMIT 1', 
                           (message.from_user.id,))
    
    if not user_ad:
        await message.answer('⚠️ Вы еще не покупали рекламу')
    else:
        user_ad = user_ad[0]
        for ad in ads:
            if user_ad[2] == ad[0]:
                end_time = datetime.datetime.strptime(user_ad[4], '%Y-%m-%d %H:%M:%S') + datetime.timedelta(hours=ad[4] + ad[5])
                formatted_time = end_time.strftime("%H:%M %d.%m.%Y")
                
                if end_time < datetime.datetime.now():
                    await message.answer('❌ В данный момент у вас нет активной рекламы')
                elif datetime.datetime.strptime(user_ad[4], '%Y-%m-%d %H:%M:%S') + datetime.timedelta(hours=ad[4]) < datetime.datetime.now():
                    await message.answer(f'⏳ В данный момент у вас нет активной рекламы, но вам нужно подождать до {formatted_time} по МСК, так как вы недавно уже брали рекламу')
                else:
                    await message.answer(
                        f'📢 Ваша реклама:\n\n'
                        f'{ad[1]}\n'
                        f'Бонус: +{ad[3]}% к доходу\n'
                        f'Активна до {formatted_time} по МСК'
                    )
                break
            


@cmd_user_router.message(Command('donate'))
async def cmd_donate(message: Message):
    user = await execute_query_one('SELECT name FROM stats WHERE userid = ?', (message.from_user.id,))
    if not user:
        await message.answer('Сначала зарегистрируйтесь - /start')
        return

    await update_data(message.from_user.username, message.from_user.id)
    await add_action(message.from_user.id, 'cmd_donate')

    markup = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text='👑 PREMIUM Статус', callback_data=f'donate_premium_{message.from_user.id}')],
        [InlineKeyboardButton(text='👨‍💻 Спонсор клуба', callback_data=f'donate_sponsor_{message.from_user.id}')],
        [InlineKeyboardButton(text='🤖 Автоматизация', callback_data=f'donate_auto_{message.from_user.id}')]
    ])

    await message.answer(
        '💎 Донат меню\n\n'
        '👑 PREMIUM Статус - увеличение дохода фермы и эксклюзивные возможности\n'
        '👨‍💻 Спонсор клуба - бонус к доходу клуба\n'
        '🤖 Автоматизация - автоворк и автоналог\n\n'
        f'Выберите интересующий вас раздел:',
        reply_markup=markup
    )



@cmd_user_router.message(Command('top'))
async def cmd_top(message: Message):
    user = await execute_query_one('SELECT name FROM stats WHERE userid = ?', (message.from_user.id,))
    if not user:
        await message.answer('Сначала зарегистрируйтесь - /start')
        return
        
    await update_data(message.from_user.username, message.from_user.id)
    await add_action(message.from_user.id, 'cmd_top')
    
    # Получаем топ-5 по балансу и доходу
    bal = await execute_query('SELECT name, bal FROM stats ORDER BY bal DESC LIMIT 5')
    income = await execute_query('SELECT name, income FROM stats ORDER BY income DESC LIMIT 5')
    
    text = '💵 Топ 5⃣ игроков по балансу:\n\n'
    
    # Топ по балансу
    num = 1
    for user_data in bal:
        text += f'{num}⃣ {user_data[0]} - {format_number_short(user_data[1], True)}$\n'
        num += 1
    
    # Топ по доходу
    text += '\n💸 Топ 5⃣ игроков по доходу:\n\n'
    
    num = 1
    for user_data in income:
        text += f'{num}⃣ {user_data[0]} - {format_number_short(user_data[1], True)}$ / 10 мин.\n'
        num += 1
    
    await message.answer(text)
    
    
@cmd_user_router.message(Command('top_franchise'))
async def cmd_top_franchise(message: Message):
    user = await execute_query_one('SELECT name FROM stats WHERE userid = ?', (message.from_user.id,))
    if not user:
        await message.answer('Сначала зарегистрируйтесь - /start')
        return
        
    await update_data(message.from_user.username, message.from_user.id)
    await add_action(message.from_user.id, 'cmd_top_franchise')
    
    # Получаем топ-10 франшиз по доходу
    franchises = await execute_query('SELECT name, income FROM networks WHERE owner_id != ? ORDER BY income DESC LIMIT 10', 
                           (ADMIN[0],))
    
    text = '💪 Топ 10 франшиз по доходу:\n\n'
    
    # Отображаем топ-10 франшиз с медалями для первых трех мест
    for i, franchise in enumerate(franchises, 1):
        franchise_name = franchise[0] if franchise[0] else "Название не установлено"
        income = franchise[1]
        
        # Определяем эмодзи для первых трех мест
        if i == 1:
            place_emoji = "🥇"
        elif i == 2:
            place_emoji = "🥈"
        elif i == 3:
            place_emoji = "🥉"
        else:
            place_emoji = f"{i}⃣"
        
        text += f'{place_emoji} {franchise_name} - {format_number_short(income, True)} 💸\n\n'
    
    # Добавляем информацию о выдаче премиума
    text += '❗ Топ 8 и 2 случайных игрока из топ-10 франшиз получат PREMIUM каждое воскресенье, в 18:00 по МСК ❗'
    
    await message.answer(text)
    
@cmd_admin_router.message(Command('delete_all_titles'))
async def cmd_delete_all_titles(message: Message):
    if message.from_user.id not in ADMIN:
        await message.answer('❌ Недостаточно прав')
        return
        
    try:
        # Получаем статистику перед удалением
        total_titles = await execute_query('SELECT COUNT(*) FROM titles')
        titles_count = total_titles[0][0] if total_titles else 0
        
        if titles_count == 0:
            await message.answer('ℹ️ В базе нет титулов для удаления')
            return
        
        # Удаляем ВСЕ титулы из таблицы titles
        await execute_update('DELETE FROM titles')
        
        # Снимаем активные титулы у ВСЕХ пользователей
        await execute_update('UPDATE stats SET title = NULL')
        
        await message.answer(
            f'✅ *Все титулы удалены!*\n\n'
            f'🗑️ Удалено титулов: *{titles_count}*\n'
            f'👤 Снято активных титулов: *у всех пользователей*\n'
            f'⏰ Время: `{datetime.datetime.now().strftime("%d.%m.%Y %H:%M")}`',
            parse_mode='Markdown'
        )
        
        logger.info(f"Admin {message.from_user.id} deleted all titles from database")
        
    except Exception as e:
        logger.error(f"Error deleting all titles: {e}")
        await message.answer('❌ Ошибка при удалении титулов')

@cmd_user_router.message(Command('unset_title'))
async def cmd_unset_title(message: Message):
    """Снять отображение титула в профиле (для всех пользователей)"""
    user = await execute_query_one('SELECT userid, title FROM stats WHERE userid = ?', (message.from_user.id,))
    if not user:
        await message.answer('Сначала зарегистрируйтесь - /start')
        return
        
    await update_data(message.from_user.username, message.from_user.id)
    await add_action(message.from_user.id, 'cmd_unset_title')
    
    # Проверяем, есть ли установленный титул
    if user[1]:
        await execute_update('UPDATE stats SET title = NULL WHERE userid = ?', (message.from_user.id,))
        await message.answer('✅ Титул скрыт из профиля')
    else:
        await message.answer('ℹ️ У вас нет активного титула')

@cmd_user_router.message(Command('promo'))
async def cmd_promo(message: Message):
    user = await execute_query_one('SELECT name, income FROM stats WHERE userid = ?', (message.from_user.id,))
    if not user:
        await message.answer('Сначала зарегистрируйтесь - /start')
        return
        
    await update_data(message.from_user.username, message.from_user.id)
    await add_action(message.from_user.id, 'cmd_promo')
    
    user_data = user
    if len(message.text.split()) > 1:
        promo_code = message.text.split()[1]
        promo = await execute_query('SELECT * FROM promos WHERE name = ?', (promo_code,))
        
        if promo:
            promo = promo[0]
            users = parse_array(promo[3])
            if message.from_user.id not in users:
                if promo[1] < promo[2]:
                    reward = ''
                    if promo[4] == 'money':
                        reward = f'{promo[5]}$'
                        await message.answer(f'Вы успешно активировали промокод! Вы получили: {reward}')
                        await execute_update('UPDATE stats SET bal = bal + ? WHERE userid = ?', (promo[5], message.from_user.id))
                    elif promo[4] == 'income':
                        reward_amount = Decimal(promo[5]) * Decimal(user_data[1]) * 6
                        await message.answer(f'✅ Вы успешно активировали промокод! Вы получили: {reward_amount}$')
                        await execute_update('UPDATE stats SET bal = bal + ? WHERE userid = ?', (float(reward_amount), message.from_user.id))
                    
                    new_users = users
                    new_users.append(message.from_user.id)
                    await execute_update('UPDATE promos SET use = ?, users = ? WHERE name = ?', 
                                 (promo[1] + 1, format_array(new_users), promo[0]))
                else:
                    await message.answer('❌ Этот промокод уже кончился')
            else:
                await message.answer('❌ Вы уже использовали этот промокод')
        else:
            await message.answer('⚠️ Такой промокод не найден')
    else:
        await message.answer('⚠️ Команду надо использовать в формате:\n /promo (промокод)')

def safe_parse_datetime(date_str):
    """Безопасное преобразование строки в datetime"""
    if not date_str:
        return None
    try:
        # Пробуем разные форматы дат
        formats = [
            '%Y-%m-%d %H:%M:%S.%f',
            '%Y-%m-%d %H:%M:%S',
            '%Y-%m-%d %H:%M',
            '%Y-%m-%d'
        ]
        
        for fmt in formats:
            try:
                return datetime.datetime.strptime(date_str, fmt)
            except ValueError:
                continue
        
        # Если ни один формат не подошел, возвращаем None
        return None
    except (ValueError, TypeError):
        return None
       
       

CHANNEL_ID = -1003246180665  # ID канала
CHAT_ID = -1003291897549     # ID чата
       
async def check_user_subscription(user_id: int, target_id: int) -> bool:
    """Проверяет, подписан ли пользователь на канал/чат"""
    try:
        member = await bot.get_chat_member(chat_id=target_id, user_id=user_id)
        is_subscribed = member.status in ['member', 'administrator', 'creator']
        logger.info(f"Subscription check for user {user_id} on {target_id}: status={member.status}, subscribed={is_subscribed}")
        return is_subscribed
    except Exception as e:
        logger.error(f"Error checking subscription for user {user_id} on {target_id}: {e}")
        return False

async def check_user_bio(user_id: int) -> bool:
    """Проверяет, есть ли тег бота в био пользователя"""
    try:
        user = await bot.get_chat(user_id)
        bio = user.bio or ""
        
        target_username = "PCClub_sBot"
        variations = [
            target_username,
            target_username.lower(),
            target_username.upper(),
            f"@{target_username}",
            f"@{target_username.lower()}",
            f"@{target_username.upper()}"
        ]
        
        for variation in variations:
            if variation in bio:
                logger.info(f"Bio tag found for user {user_id}: {variation}")
                return True
        
        logger.info(f"Bio tag NOT found for user {user_id}. Bio: {bio}")
        return False
        
    except Exception as e:
        logger.error(f"Error checking bio for user {user_id}: {e}")
        return False

async def update_all_bonuses(user_id: int):
    """Обновляет статус всех бонусов"""
    try:
        channel_subscribed = await check_user_subscription(user_id, CHANNEL_ID)
        chat_subscribed = await check_user_subscription(user_id, CHAT_ID)
        bio_checked = await check_user_bio(user_id)
        
        logger.info(f"Bonus check for user {user_id}: channel={channel_subscribed}, chat={chat_subscribed}, bio={bio_checked}")
        
        # Получаем текущий статус
        current_status = await execute_query_one(
            'SELECT channel_subscribed, chat_subscribed, bio_checked FROM user_social_bonus WHERE user_id = ?',
            (user_id,)
        )
        
        # Если статус изменился, обновляем
        if (not current_status or 
            current_status[0] != channel_subscribed or 
            current_status[1] != chat_subscribed or
            current_status[2] != bio_checked):
            
            await execute_update('''
            INSERT OR REPLACE INTO user_social_bonus 
            (user_id, channel_subscribed, chat_subscribed, bio_checked, last_check)
            VALUES (?, ?, ?, ?, ?)
            ''', (user_id, channel_subscribed, chat_subscribed, bio_checked, datetime.datetime.now().isoformat()))
            
        return channel_subscribed, chat_subscribed, bio_checked
        
    except Exception as e:
        logger.error(f"Error updating bonuses for user {user_id}: {e}")
        return False, False, False

async def get_social_bonus(user_id: int) -> float:
    """Возвращает текущий бонус от подписок и био в процентах"""
    try:
        result = await execute_query_one(
            'SELECT channel_subscribed, chat_subscribed, bio_checked FROM user_social_bonus WHERE user_id = ?',
            (user_id,)
        )

        if not result:
            # Автоматически проверяем и создаем запись
            channel_sub, chat_sub, bio_checked = await update_all_bonuses(user_id)
            result = (channel_sub, chat_sub, bio_checked)

        channel_bonus = 0.05 if result[0] else 0.0    # +5% за канал
        chat_bonus = 0.05 if result[1] else 0.0       # +5% за чат
        bio_bonus = 0.05 if result[2] else 0.0        # +5% за био

        return channel_bonus + chat_bonus + bio_bonus

    except Exception as e:
        logger.error(f"Error getting social bonus: {e}")
        return 0.0

# ===== SOCIAL BONUS COMMAND =====
@cmd_user_router.message(Command('social'))
async def cmd_social(message: Message):
    """Показывает статус социальных бонусов"""
    user = await execute_query_one('SELECT name FROM stats WHERE userid = ?', (message.from_user.id,))
    if not user:
        await message.answer('Сначала зарегистрируйтесь - /start')
        return
        
    await update_data(message.from_user.username, message.from_user.id)
    await add_action(message.from_user.id, 'cmd_social')
    
    # Обновляем все бонусы
    channel_sub, chat_sub, bio_checked = await update_all_bonuses(message.from_user.id)
    total_bonus = await get_social_bonus(message.from_user.id)
    
    # Создаем клавиатуру
    markup = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text='🔄 Обновить всё', callback_data=f'social_refresh_{message.from_user.id}')]
    ])
    
    text = (
        "🌟 <b>Социальные бонусы</b>\n\n"
        
        "📊 <b>Ваши бонусы:</b>\n"
        f"📢 Канал: {'✅ +5%' if channel_sub else '❌ 0%'}\n"
        f"💬 Чат: {'✅ +5%' if chat_sub else '❌ 0%'}\n"
        f"👤 Био: {'✅ +5%' if bio_checked else '❌ 0%'}\n\n"
        
        f"💰 <b>Общий бонус: +{total_bonus * 100:.1f}% к доходу</b>\n\n"
        
        "📈 <b>Как получить бонусы:</b>\n"
        "• Подпишись на канал: +5%\n"
        "• Вступи в чат: +5%\n"
        "• Добавь в био @PCClub_sBot: +5%\n\n"
        
        "💡 <b>Инструкция:</b>\n"
        "1. Подпишись на канал и чат\n"
        "2. Добавь @PCClub_sBot в био Telegram\n"
        "3. Нажми кнопку ниже для проверки\n"
        "4. Получай +15% к доходу!"
    )
    
    await message.answer(text, reply_markup=markup, parse_mode='HTML')

# ===== SOCIAL BONUS CALLBACK HANDLER =====
@callback_router.callback_query(F.data.startswith('social_refresh_'))
async def cb_social_refresh(callback: CallbackQuery):
    userid = callback.data.split('_')[-1]
    user = await execute_query_one('SELECT userid FROM stats WHERE userid = ?', (callback.from_user.id,))
    
    if not user or user[0] != int(userid):
        await callback.answer('⚠️ Это не твое сообщение', show_alert=True)
        return
        
    await update_data(callback.from_user.username, callback.from_user.id)
    await add_action(callback.from_user.id, 'cb_social_refresh')
    
    # Обновляем все бонусы
    channel_sub, chat_sub, bio_checked = await update_all_bonuses(callback.from_user.id)
    total_bonus = await get_social_bonus(callback.from_user.id)
    
    # Создаем обновленную клавиатуру
    markup = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text='🔄 Обновить всё', callback_data=f'social_refresh_{callback.from_user.id}')]
    ])
    
    text = (
        "🌟 <b>Социальные бонусы</b>\n\n"
        
        "📊 <b>Ваши бонусы:</b>\n"
        f"📢 Канал: {'✅ +5%' if channel_sub else '❌ 0%'}\n"
        f"💬 Чат: {'✅ +5%' if chat_sub else '❌ 0%'}\n"
        f"👤 Био: {'✅ +5%' if bio_checked else '❌ 0%'}\n\n"
        
        f"💰 <b>Общий бонус: +{total_bonus * 100:.1f}% к доходу</b>\n\n"
        
        "📈 <b>Как получить бонусы:</b>\n"
        "• Подпишись на канал: +5%\n"
        "• Вступи в чат: +5%\n"
        "• Добавь в био @PCClub_sBot: +5%\n\n"
        
        "✅ <b>Статус обновлен!</b>"
    )
    
    await callback.message.edit_text(text, reply_markup=markup, parse_mode='HTML')
    await callback.answer('✅ Статус бонусов обновлен!')   
       
       
       
       
def format_number_short(number: float, is_usd: bool = False) -> str:
    """
    Сокращает большие числа для лучшей читаемости с русскими сокращениями
    """
    if number == 0:
        return "0"
    
    # Для BTC оставляем больше знаков после запятой
    if not is_usd:
        if number < 0.001:
            return f"{number:.6f}"
        elif number < 1:
            return f"{number:.4f}"
        elif number < 1000:
            return f"{number:.3f}"
    
    abs_number = abs(number)
    sign = "-" if number < 0 else ""
    
    if abs_number < 1000:
        if is_usd:
            return f"{sign}{abs_number:,.0f}".replace(',', ' ')
        return f"{sign}{abs_number:.3f}"
    
    elif abs_number < 1_000_000:  # Тысячи
        formatted = f"{abs_number/1000:.2f}".rstrip('0').rstrip('.')
        return f"{sign}{formatted} тыс."
    
    elif abs_number < 1_000_000_000:  # Миллионы
        formatted = f"{abs_number/1_000_000:.2f}".rstrip('0').rstrip('.')
        return f"{sign}{formatted} млн"
    
    elif abs_number < 1_000_000_000_000:  # Миллиарды
        formatted = f"{abs_number/1_000_000_000:.2f}".rstrip('0').rstrip('.')
        return f"{sign}{formatted} млрд"
    
    elif abs_number < 1_000_000_000_000_000:  # Триллионы
        formatted = f"{abs_number/1_000_000_000_000:.2f}".rstrip('0').rstrip('.')
        return f"{sign}{formatted} трлн"
    
    elif abs_number < 1_000_000_000_000_000_000:  # Квадриллионы
        formatted = f"{abs_number/1_000_000_000_000_000:.2f}".rstrip('0').rstrip('.')
        return f"{sign}{formatted} квадрлн"
    
    elif abs_number < 1e18:  # Квинтиллионы
        formatted = f"{abs_number/1e15:.2f}".rstrip('0').rstrip('.')
        return f"{sign}{formatted} квинтлн"
    
    elif abs_number < 1e21:  # Секстиллионы
        formatted = f"{abs_number/1e18:.2f}".rstrip('0').rstrip('.')
        return f"{sign}{formatted} секстилн"
    
    elif abs_number < 1e24:  # Септиллионы
        formatted = f"{abs_number/1e21:.2f}".rstrip('0').rstrip('.')
        return f"{sign}{formatted} септилн"
    
    elif abs_number < 1e27:  # Октиллионы
        formatted = f"{abs_number/1e24:.2f}".rstrip('0').rstrip('.')
        return f"{sign}{formatted} октилн"
    
    elif abs_number < 1e30:  # Нониллионы
        formatted = f"{abs_number/1e27:.2f}".rstrip('0').rstrip('.')
        return f"{sign}{formatted} нонилн"
    
    else:  # Дециллионы и больше
        formatted = f"{abs_number/1e30:.2f}".rstrip('0').rstrip('.')
        return f"{sign}{formatted} децилн"


# ===== USER INCOME CALCULATION FUNCTION =====

async def calculate_user_income(user_id: int) -> dict:
    """
    Рассчитывает итоговый доход пользователя с учетом всех бонусов, включая экспансии
    """
    try:
        # Получаем базовые данные пользователя
        stats = await execute_query_one(
            'SELECT income, premium FROM stats WHERE userid = ?', 
            (user_id,)
        )
        
        if not stats:
            return {
                'base_income': Decimal('0'),
                'final_income': Decimal('0'),
                'has_premium': False,
                'expansion_bonus': Decimal('0')
            }
        
        base_income = Decimal(str(stats[0]))

        # Начинаем с базового дохода
        final_income = base_income
        expansion_bonus = Decimal('0')  # Инициализируем переменную

        # Бонус от экспансии (только к чистому доходу)
        expansion_bonus_percent = await get_expansion_bonus(user_id)
        if expansion_bonus_percent > 0:
            expansion_bonus = base_income * Decimal(str(expansion_bonus_percent))
            final_income += expansion_bonus

        # Добавляем бонус от репутации
        rep_income_bonus, _ = await get_reputation_bonuses(user_id)
        if rep_income_bonus > 0:
            reputation_bonus = base_income * Decimal(str(rep_income_bonus))
            final_income += reputation_bonus

        # Добавляем социальные бонусы
        social_bonus_percent = await get_social_bonus(user_id)
        if social_bonus_percent > 0:
            social_bonus = base_income * Decimal(str(social_bonus_percent))
            final_income += social_bonus

        # Проверяем PREMIUM статус
        has_premium = False
        premium_date = safe_parse_datetime(stats[1])
        if premium_date and premium_date > datetime.datetime.now():
            has_premium = True
            premium_bonus = base_income * Decimal('0.35')  # +35% за премиум
            final_income += premium_bonus

        # Применяем улучшения
        upgrades = await execute_query_one(
            'SELECT upgrade_internet, upgrade_devices, upgrade_service FROM stats WHERE userid = ?',
            (user_id,)
        )

        if upgrades:
            upgrade_bonus = sum(upgrades) / 100.0
            final_income += base_income * Decimal(str(upgrade_bonus))

        # Применяем активную рекламу
        user_ad = await execute_query_one(
            'SELECT num, percent, dt FROM ads WHERE userid = ? ORDER BY dt DESC LIMIT 1',
            (user_id,)
        )

        if user_ad:
            for ad in ads:
                if user_ad[0] == ad[0]:
                    ad_dt = safe_parse_datetime(user_ad[2])
                    if ad_dt and ad_dt + datetime.timedelta(hours=ad[4]) > datetime.datetime.now():
                        ad_bonus = base_income * Decimal(str(user_ad[1])) / Decimal('100')
                        final_income += ad_bonus
                    break

        # Бонус от событий
        event_bonus = await get_event_bonus(user_id)
        if event_bonus > 0:
            event_income = base_income * Decimal(str(event_bonus))
            final_income += event_income

        # В конце применяем бустер дохода (income booster) ко ВСЕМУ итоговому доходу
        final_income = await apply_boosters(user_id, final_income)
        
        return {
            'base_income': base_income,
            'final_income': final_income,
            'has_premium': has_premium,
            'expansion_bonus': expansion_bonus
        }
        
    except Exception as e:
        logger.error(f"Error calculating user income for {user_id}: {e}")
        return {
            'base_income': Decimal('0'),
            'final_income': Decimal('0'),
            'has_premium': False,
            'expansion_bonus': Decimal('0')
        }
    
    
    
async def add_booster_to_user(user_id: int, booster_type: str, days: int) -> bool:
    """Добавить бустер пользователю"""
    try:
        end_date = datetime.datetime.now() + datetime.timedelta(days=days)

        if booster_type == "income":
            await execute_update(
                'UPDATE stats SET income_booster_end = ? WHERE userid = ?',
                (end_date, user_id)
            )
        elif booster_type == "auto":
            await execute_update(
                'UPDATE stats SET auto_booster_end = ? WHERE userid = ?',
                (end_date, user_id)
            )
        elif booster_type == "premium":
            await execute_update(
                'UPDATE stats SET premium = ? WHERE userid = ?',
                (end_date, user_id)
            )
        else:
            return False

        return True
    except Exception as e:
        logger.error(f"Error adding booster to user {user_id}: {e}")
        return False

async def remove_booster_from_user(user_id: int, booster_type: str) -> bool:
    """Удалить бустер у пользователя"""
    try:
        if booster_type == "income":
            await execute_update(
                'UPDATE stats SET income_booster_end = NULL WHERE userid = ?',
                (user_id,)
            )
        elif booster_type == "auto":
            await execute_update(
                'UPDATE stats SET auto_booster_end = NULL WHERE userid = ?',
                (user_id,)
            )
        elif booster_type == "premium":
            await execute_update(
                'UPDATE stats SET premium = NULL WHERE userid = ?',
                (user_id,)
            )
        else:
            return False

        return True
    except Exception as e:
        logger.error(f"Error removing booster from user {user_id}: {e}")
        return False

async def get_active_boosters(user_id: int) -> dict:
    """Получить активные бустеры пользователя"""
    try:
        user_stats = await execute_query_one(
            'SELECT income_booster_end, auto_booster_end, premium FROM stats WHERE userid = ?',
            (user_id,)
        )

        if not user_stats:
            return {}

        active_boosters = {}
        now = datetime.datetime.now()

        # Проверяем бустер дохода
        income_booster_end = safe_parse_datetime(user_stats[0])
        if income_booster_end and income_booster_end > now:
            active_boosters["income"] = {
                "end_date": income_booster_end,
                "days_left": (income_booster_end - now).days
            }

        # Проверяем бустер автоматизации
        auto_booster_end = safe_parse_datetime(user_stats[1])
        if auto_booster_end and auto_booster_end > now:
            active_boosters["auto"] = {
                "end_date": auto_booster_end,
                "days_left": (auto_booster_end - now).days
            }

        # Проверяем PREMIUM статус
        premium_end = safe_parse_datetime(user_stats[2])
        if premium_end and premium_end > now:
            active_boosters["premium"] = {
                "end_date": premium_end,
                "days_left": (premium_end - now).days
            }

        return active_boosters

    except Exception as e:
        logger.error(f"Error getting active boosters for user {user_id}: {e}")
        return {}

async def cleanup_expired_boosters():
    """Очистка истекших бустеров"""
    try:
        now = datetime.datetime.now()
        
        # Очищаем истекшие бустеры дохода
        await execute_update(
            'UPDATE stats SET income_booster_end = NULL WHERE income_booster_end < ?',
            (now,)
        )
        
        # Очищаем истекшие бустеры автоматизации
        await execute_update(
            'UPDATE stats SET auto_booster_end = NULL WHERE auto_booster_end < ?', 
            (now,)
        )
        
        logger.info("Expired boosters cleaned up successfully")
        
    except Exception as e:
        logger.error(f"Error cleaning up expired boosters: {e}")

# ===== ОБНОВЛЯЕМ ФУНКЦИЮ РАСЧЕТА ДОХОДА =====
async def apply_boosters(user_id: int, base_income: Decimal) -> Decimal:
    """Применить бустеры к доходу"""
    try:
        # Проверяем активные бустеры
        active_boosters = await get_active_boosters(user_id)
        final_income = base_income
        
        # Применяем бустер дохода +25%
        if "income" in active_boosters:
            income_bonus = base_income * Decimal('0.25')
            final_income += income_bonus
            logger.info(f"Income booster applied for user {user_id}: +{income_bonus}$")
        
        return final_income
        
    except Exception as e:
        logger.error(f"Error applying boosters for user {user_id}: {e}")
        return base_income

# ===== UPDATED PROFILE COMMAND =====
@cmd_user_router.message(Command('me'))
async def cmd_profile(message: Message):
    user = await execute_query_one(
        'SELECT name, taxes, bonus FROM stats WHERE userid = ?', 
        (message.from_user.id,)
    )
    if not user:
        await message.answer('Сначала зарегистрируйтесь - /start')
        return
        
    await update_data(message.from_user.username, message.from_user.id)
    await add_action(message.from_user.id, 'cmd_profile')
    
    # Получаем основные данные пользователя
    stats = await execute_query_one(
        'SELECT name, pc, room, bal, income, network, premium, title FROM stats WHERE userid = ?', 
        (message.from_user.id,)
    )
    
    if not stats:
        await message.answer('Ошибка получения данных профиля')
        return
    
    # Рассчитываем доход через отдельную функцию
    income_data = await calculate_user_income(message.from_user.id)
    
    network = await execute_query_one(
        'SELECT name FROM networks WHERE owner_id = ?', 
        (stats[5],)
    ) if stats[5] else None
    
    # === ИНФОРМАЦИЯ О ПРЕМИУМЕ ===
    premium_info = ""
    if income_data['has_premium']:
        premium_date = safe_parse_datetime(stats[6])
        if premium_date:
            if premium_date.date() == datetime.datetime.now().date():
                premium_expire = "Сегодня"
            elif premium_date.date() == (datetime.datetime.now() + datetime.timedelta(days=1)).date():
                premium_expire = "Завтра"
            else:
                premium_expire = premium_date.strftime("%d.%m.%Y")
            
            premium_info = f"👑 PREMIUM 👑\nСрок: {premium_expire}\n\n"
    
    # === ИНФОРМАЦИЯ О СОБЫТИИ ===
    active_event = await get_active_event(message.from_user.id)
    event_info = ""
    
    if active_event:
        event_type, bonus_percent, end_time = active_event
        event_name = next((e["name"] for e in EVENTS if e["type"] == event_type), "Событие")
        
        # Вычисляем оставшееся время
        time_left = safe_parse_datetime(end_time) - datetime.datetime.now()
        hours_left = int(time_left.total_seconds() // 3600)
        minutes_left = int((time_left.total_seconds() % 3600) // 60)
        
        event_info = f"🎯 {event_name}: +{bonus_percent}% к доходу\n⏰ Осталось: {hours_left}ч {minutes_left}м\n\n"
    
    # === ОТОБРАЖЕНИЕ ТИТУЛА ===
    title_info = ""
    if stats[7]:  # Если есть установленный титул
        # Экранируем специальные символы Markdown в титуле
        title_text = stats[7].replace('*', '\\*').replace('_', '\\_').replace('`', '\\`').replace('[', '\\[')
        title_info = f"*{title_text}*\n\n"
    
    # === ОСНОВНОЙ ТЕКСТ ПРОФИЛЯ ===
    # Экранируем имя пользователя от специальных символов Markdown
    user_name = stats[0].replace('*', '\\*').replace('_', '\\_').replace('`', '\\`').replace('[', '\\[')
    
    text = premium_info  # Добавляем премиум информацию в начало
    
    text += f"👤 *Профиль:*\n{user_name}\n"
    
    # Информация о титуле (если есть)
    if title_info:
        text += title_info

    # Информация о событии (если есть)
    if event_info:
        text += event_info
    
    # Оборудование и комната
    text += f"🖥️ Компьютеры: *{stats[1]}/{stats[2] * 5}*\n"
    text += f"🏠 Уровень комнаты: *{stats[2]}*\n\n"
    
    # Финансы (используем данные из функции расчета дохода)
    text += f"💳 Баланс: *{format_number_short(stats[3], True)}$*\n"
    text += f"📈 Доход: *{format_number_short(income_data['final_income'], True)}$ / 10 мин*\n"
    text += f"💰 Чистый доход: *{format_number_short(income_data['base_income'], True)}$ / 10 мин*\n\n"
    
    # Франшиза
    if network:
        text += f"🌐 Франшиза: *{network[0]}*\n\n"
    else:
        text += "🌐 Не состоит в франшизе\n\n"
    
    # Полезные команды
    text += "📝 *Полезные команды:*\n"
    text += "• Сменить ник - /nickname\n"
    text += "• Статистика - /stats\n"
    text += "• Работа - /work\n"
    text += "• Титулы - /titles\n"
    text += "• Репутация - /reputation\n"
    text += "• Бонусы - /social"
    
    # Бонусная кнопка
    if user[2] == 1:
        markup = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(
                text='🎁 Получить ежедневный бонус', 
                callback_data=f'bonus_{message.from_user.id}'
            )]
        ])
        await message.answer(text, reply_markup=markup, parse_mode='Markdown')
    else:
        await message.answer(text, parse_mode='Markdown')
        
@cmd_user_router.message(Command('set_title'))
async def cmd_set_title(message: Message):
    user = await execute_query_one('SELECT name FROM stats WHERE userid = ?', (message.from_user.id,))
    if not user:
        await message.answer('Сначала зарегистрируйтесь - /start')
        return
        
    await update_data(message.from_user.username, message.from_user.id)
    await add_action(message.from_user.id, 'cmd_set_title')
    
    text_parts = message.text.split(' ')
    if len(text_parts) == 2:
        title = await execute_query('SELECT * FROM titles WHERE id = ?', (text_parts[1],))
        if title:
            title = title[0]
            users = parse_array(title[1])
            if message.from_user.id in users:
                await message.answer('🎖️ Вы успешно установили титул')
                await execute_update('UPDATE stats SET title = ? WHERE userid = ?', (title[0], message.from_user.id))
            else:
                await message.answer('⚠️ Этот титул вам не доступен')
        else:
            await message.answer('❌ Такой титул не найден')
    else:
        await message.answer('⚠️ Команду нужно вводить в формате: /set_title (id титула*)')

@cmd_admin_router.message(Command('add_title'))
async def cmd_add_title(message: Message):
    if message.from_user.id not in ADMIN:
        await message.answer('❌ Недостаточно прав')
        return
        
    # Разбираем команду: /add_title ID_пользователя [Название титула]
    import re
    match = re.match(r'/add_title\s+(\d+)\s+\[(.+)\]', message.text)
    
    if match:
        target_user_id = int(match.group(1))
        title_name = match.group(2).strip()  # Берем текст без скобок
        
        # Проверяем существование пользователя
        user = await execute_query_one('SELECT userid, name FROM stats WHERE userid = ?', (target_user_id,))
        if not user:
            await message.answer('❌ Пользователь не найден')
            return
        
        # Генерируем уникальный ID для титула
        while True:
            title_id = str(random.randint(1000, 9999))
            existing_title = await execute_query('SELECT * FROM titles WHERE id = ?', (title_id,))
            if not existing_title:
                break
        
        # Создаем новый титул
        await execute_update('INSERT INTO titles (name, users, id) VALUES (?, ?, ?)', 
                     (title_name, format_array([target_user_id]), title_id))
        
        await message.answer(
            f'✅ *Титул создан!*\n\n'
            f'👤 Пользователь: *{user[1]}*\n'
            f'🎖️ Титул: *{title_name}*\n'
            f'🔑 ID титула: `{title_id}`',
            parse_mode='Markdown'
        )
            
    else:
        await message.answer(
            '❓ *Используйте:* `/add_title ID_пользователя [Название титула]`\n\n'
            '*Пример:*\n'
            '`/add_title 5929120983 [Я мопс, мне похуй]`',
            parse_mode='Markdown'
        )

@cmd_user_router.message(Command('titles'))
async def cmd_titles(message: Message):
    user = await execute_query_one('SELECT name FROM stats WHERE userid = ?', (message.from_user.id,))
    if not user:
        await message.answer('Сначала зарегистрируйтесь - /start')
        return
        
    await update_data(message.from_user.username, message.from_user.id)
    await add_action(message.from_user.id, 'cmd_titles')
    
    titles = await execute_query('SELECT * FROM titles WHERE users LIKE ?', (f'%{message.from_user.id}%',))
    
    if not titles:
        await message.answer('🎖️ *У вас пока нет титулов*', parse_mode='Markdown')
        return
    
    text = '🎖️ *Ваши титулы:*\n\n'
    for i, title in enumerate(titles, 1):
        text += f'{i}) *{title[0]}*\n'
        text += f'Установить: `/set_title {title[2]}`\n\n'
    
    text += '💡 *Чтобы снять титул из профиля:*\n`/unset_title`'
    
    await message.answer(text, parse_mode='Markdown')

@cmd_user_router.message(Command('cancel'))
async def cmd_cancel(message: Message, state: FSMContext):
    user = await execute_query_one('SELECT name FROM stats WHERE userid = ?', (message.from_user.id,))
    if not user:
        await message.answer('Сначала зарегистрируйтесь - /start')
        return
        
    await update_data(message.from_user.username, message.from_user.id)
    await add_action(message.from_user.id, 'cmd_cancel')
    await state.clear()
    await message.answer('❌ Действие отменено')

# ===== UPGRADES HANDLERS =====

@cmd_upgrades_router.message(Command('upgrades'))
async def cmd_upgrades(message: Message):
    user = await execute_query_one('SELECT name, upgrade_internet, upgrade_devices, upgrade_service FROM stats WHERE userid = ?', 
                        (message.from_user.id,))
    if not user:
        await message.answer('Сначала зарегистрируйтесь - /start')
        return
        
    await update_data(message.from_user.username, message.from_user.id)
    await add_action(message.from_user.id, 'cmd_upgrades')
    
    user_data = user
    text = '🔧 Улучшения отеля:'
    els = [
        [1, '📶 Интернет', 'upgrade_internet', user_data[1]],
        [2, '💻 Девайсы', 'upgrade_devices', user_data[2]],
        [3, '⭐ Сервис', 'upgrade_service', user_data[3]]
    ]
    
    total_bonus = 0
    
    for el in els:
        current_level = el[3]
        total_bonus += current_level
        
        # Проверяем, достигнут ли максимум
        if current_level == 5:
            text += f'\n\n{el[1]}: {current_level}/5 (+{current_level}%) - максимум'
        else:
            # Ищем стоимость следующего улучшения
            for upg in upgrade:
                if current_level + 1 == upg[0]:
                    text += f'\n\n{el[1]}: {current_level}/5 (+{current_level}%)\nСледующий уровень: {upg[1]}$ - /{el[2]}'
                    break
    
    text += f'\n\n📊 Общий бонус от улучшений: +{total_bonus}% к доходу'
    
    await message.answer(text)
    
@cmd_upgrades_router.message(Command('upgrade_internet'))
async def cmd_upgrade_internet(message: Message):
    await upgrade_handler(message, 'upgrade_internet')

@cmd_upgrades_router.message(Command('upgrade_devices'))
async def cmd_upgrade_devices(message: Message):
    await upgrade_handler(message, 'upgrade_devices')

@cmd_upgrades_router.message(Command('upgrade_service'))
async def cmd_upgrade_service(message: Message):
    await upgrade_handler(message, 'upgrade_service')

async def upgrade_handler(message: Message, upgrade_type: str):
    user = await execute_query_one(f'SELECT name, bal, {upgrade_type} FROM stats WHERE userid = ?', 
                        (message.from_user.id,))
    if not user:
        await message.answer('Сначала зарегистрируйтесь - /start')
        return
        
    await update_data(message.from_user.username, message.from_user.id)
    await add_action(message.from_user.id, f'cmd_{upgrade_type}')
    
    user_data = user
    current_level = user_data[2]
    user_balance = user_data[1]
    
    upgrade_names = {
        'upgrade_internet': '📶 Интернет',
        'upgrade_devices': '💻 Девайсы', 
        'upgrade_service': '⭐ Сервис'
    }
    upgrade_name = upgrade_names.get(upgrade_type, upgrade_type.replace('_', ' '))
    
    for upg in upgrade:
        if upg[0] == current_level + 1:
            if current_level != 10:
                if user_balance >= upg[1]:
                    await execute_update(f'UPDATE stats SET {upgrade_type} = {upgrade_type} + 1, bal = bal - ? WHERE userid = ?', 
                                 (upg[1], message.from_user.id))
                    
                    await message.answer(
                        f'✅ {upgrade_name} улучшен!\n'
                        f'Уровень: {current_level} → {current_level + 1}\n'
                        f'Бонус: +{current_level}% → +{current_level + 1}%'
                    )
                else:
                    await message.answer(
                        f'❌ Недостаточно средств\n'
                        f'Нужно: {upg[1]}$\n'
                        f'У вас: {user_balance}$'
                    )
            else:
                await message.answer('⚠️ Достигнут максимальный уровень')
            break

# ===== GAMES HANDLERS =====
@cmd_games_router.message(F.text == '🎮 Игры')
async def msg_casino(message: Message):
    await cmd_casino(message)

@cmd_games_router.message(Command('games'))
async def cmd_casino(message: Message):
    user = await execute_query_one('SELECT name FROM stats WHERE userid = ?', (message.from_user.id,))
    if not user:
        await message.answer('Сначала зарегистрируйтесь - /start')
        return
        
    await update_data(message.from_user.username, message.from_user.id)
    await add_action(message.from_user.id, 'cmd_casino')
    
    if message.chat.type == 'private':
        markup = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text='🪙 Монетка', callback_data=f'game_1_{message.from_user.id}')],
            [InlineKeyboardButton(text='🎲 Кубик', callback_data=f'game_2_{message.from_user.id}')]
        ])
        await message.answer('🎮 Какую игру вы хотите сыграть?', reply_markup=markup)
    else:
        await message.answer('🎮 Какую игру вы хотите сыграть?\n🪙 Монетка - !game1 (ставка) (сумма ставки)\n🎲 Кубик - !game2 (ставка) (сумма ставки)')

@cmd_games_router.message(Command('dice'))
async def cmd_casino_chat(message: Message):
    sent_dice = await message.answer_dice(emoji='🎲')
    await asyncio.sleep(4)
    dice_value = sent_dice.dice.value
    await message.answer(f'🎲 Результат: {dice_value}')

@cmd_games_router.message(F.text.startswith('!game1'))
async def cmd_game1_chat(message: Message):
    user = await execute_query_one('SELECT name, bal FROM stats WHERE userid = ?', (message.from_user.id,))
    if not user:
        await message.answer('Сначала зарегистрируйтесь - /start')
        return
        
    await update_data(message.from_user.username, message.from_user.id)
    await add_action(message.from_user.id, 'cmd_game1_chat')
    
    user_data = user
    command = message.text[1:].split(' ')
    if len(command) == 3 and command[2].isdigit() and command[1].lower() in ['орел', 'решка', 'орёл']:
        if int(command[2]) >= 5000:
            if int(command[2]) <= user_data[1]:
                value = random.randint(1, 100)
                if value <= 49:
                    await execute_update('UPDATE stats SET bal = bal + ? WHERE userid = ?', (int(command[2]), message.from_user.id))
                    await message.answer(f'🎊 Вы угадали и получаете {int(command[2])*2}$')
                else:
                    await execute_update('UPDATE stats SET bal = bal - ? WHERE userid = ?', (int(command[2]), message.from_user.id))
                    await message.answer(f'💥 Вы не угадали и теряете {command[2]}$')
            else:
                await message.answer('❌ У вас не хватает $')
        else:
            await message.answer('❌ Минимальная ставка 5000')
    else:
        await message.answer('⚠️ Команду нужно использовать в формате:\n!game1 (орел/решка*) (целое число*)')

@cmd_games_router.message(F.text.startswith('!game2'))
async def cmd_game2_chat(message: Message):
    user = await execute_query_one('SELECT name, bal FROM stats WHERE userid = ?', (message.from_user.id,))
    if not user:
        await message.answer('Сначала зарегистрируйтесь - /start')
        return
        
    await update_data(message.from_user.username, message.from_user.id)
    await add_action(message.from_user.id, 'cmd_game2_chat')
    
    user_data = user
    command = message.text[1:].split(' ')
    if len(command) == 3 and command[1].isdigit() and int(command[1]) in [1, 2, 3, 4, 5, 6] and command[2].isdigit():
        if int(command[2]) >= 5000:
            if int(command[2]) <= user_data[1]:
                sent_dice = await message.answer_dice(emoji='🎲')
                dice_value = sent_dice.dice.value
                if dice_value == int(command[1]):
                    await execute_update('UPDATE stats SET bal = bal + ? WHERE userid = ?', (int(command[2])*5, message.from_user.id))
                    await asyncio.sleep(3)
                    await message.answer(f'🎊 Вы угадали и получаете {int(command[2])*6}$')
                else:
                    await execute_update('UPDATE stats SET bal = bal - ? WHERE userid = ?', (int(command[2]), message.from_user.id))
                    await asyncio.sleep(3)
                    await message.answer(f'💥 Вы не угадали и теряете {command[2]}$')
            else:
                await message.answer('❌ У вас не хватает $')
        else:
            await message.answer('❌ Минимальная ставка 5000')
    else:
        await message.answer('⚠️ Команду нужно использовать в формате:\n!game2 (число от 1 до 6*) (целое число*)')

# ===== FRANCHISE HANDLERS =====
@cmd_franchise_router.message(F.text == '🌐 Франшизы')
async def msg_franchise(message: Message):
    await cmd_franchise(message)

@cmd_franchise_router.message(Command('allow_user'))
async def cmd_allow_user(message: Message):
    user = await execute_query_one('SELECT name, network FROM stats WHERE userid = ?', (message.from_user.id,))
    if not user:
        await message.answer('Сначала зарегистрируйтесь - /start')
        return
        
    await update_data(message.from_user.username, message.from_user.id)
    await add_action(message.from_user.id, 'cmd_allow_user')
    
    user_data = user
    if len(message.text.split()) > 1 and message.text.split()[1].isdigit():
        target_user = int(message.text.split()[1])
        admins_result = await execute_query('SELECT admins FROM networks WHERE owner_id = ?', (user_data[1],))
        requests_result = await execute_query('SELECT requests FROM networks WHERE owner_id = ?', (user_data[1],))
        
        if user_data[1] is not None and admins_result and requests_result:
            admins = parse_array(admins_result[0][0])
            requests = parse_array(requests_result[0][0])
            
            if target_user in requests:
                if message.from_user.id in admins or message.from_user.id == user_data[1]:
                    net_user = await execute_query('SELECT network FROM stats WHERE userid = ?', (target_user,))
                    if not net_user or net_user[0][0] is None:
                        await message.answer('✅ Вы успешно приняли заявку')
                        await bot.send_message(target_user, '🎊 Вы приняты в франшизу')
                        await execute_update('UPDATE stats SET network = ? WHERE userid = ?', (user_data[1], target_user))
                        
                        new_requests = requests
                        new_requests.remove(target_user)
                        await execute_update('UPDATE networks SET requests = ? WHERE owner_id = ?', 
                                     (format_array(new_requests), user_data[1]))
                    else:
                        await message.answer('❌ Пользователь уже состоит в другой франшизе')
                        new_requests = requests
                        new_requests.remove(target_user)
                        await execute_update('UPDATE networks SET requests = ? WHERE owner_id = ?', 
                                     (format_array(new_requests), user_data[1]))
                else:
                    await message.answer('❌ Вы не являетесь владельцем франшизы или её администратором')
            else:
                await message.answer('⚠️ Этот пользователь не отправлял заявку в вашу франшизу')
        else:
            await message.answer('❌ Вы не состоите в франшизе')
    else:
        await message.answer('⚠️ Используйте: /allow_user (ID пользователя)')

@cmd_franchise_router.message(Command('reject_user'))
async def cmd_reject_user(message: Message):
    user = await execute_query_one('SELECT name, network FROM stats WHERE userid = ?', (message.from_user.id,))
    if not user:
        await message.answer('Сначала зарегистрируйтесь - /start')
        return
        
    await update_data(message.from_user.username, message.from_user.id)
    await add_action(message.from_user.id, 'cmd_reject_user')
    
    user_data = user
    if len(message.text.split()) > 1 and message.text.split()[1].isdigit():
        target_user = int(message.text.split()[1])
        admins_result = await execute_query('SELECT admins FROM networks WHERE owner_id = ?', (user_data[1],))
        requests_result = await execute_query('SELECT requests FROM networks WHERE owner_id = ?', (user_data[1],))
        
        if user_data[1] is not None and admins_result and requests_result:
            admins = parse_array(admins_result[0][0])
            requests = parse_array(requests_result[0][0])
            
            if target_user in requests:
                if message.from_user.id in admins or message.from_user.id == user_data[1]:
                    await message.answer('✅ Вы успешно отклонили заявку')
                    new_requests = requests
                    new_requests.remove(target_user)
                    await execute_update('UPDATE networks SET requests = ? WHERE owner_id = ?', 
                                 (format_array(new_requests), user_data[1]))
                else:
                    await message.answer('❌ Вы не являетесь владельцем франшизы или её администратором')
            else:
                await message.answer('⚠️ Этот пользователь не отправлял заявку в вашу франшизу')
        else:
            await message.answer('❌ Вы не состоите в франшизе')
    else:
        await message.answer('⚠️ Используйте: /reject_user (ID пользователя)')

@cmd_franchise_router.message(Command('set_admin'))
async def cmd_set_admin(message: Message):
    user = await execute_query_one('SELECT name, network FROM stats WHERE userid = ?', (message.from_user.id,))
    if not user:
        await message.answer('Сначала зарегистрируйтесь - /start')
        return
        
    await update_data(message.from_user.username, message.from_user.id)
    await add_action(message.from_user.id, 'cmd_set_admin')
    
    user_data = user
    if len(message.text.split()) > 1 and message.text.split()[1].isdigit():
        target_user = int(message.text.split()[1])
        
        if target_user == message.from_user.id:
            await message.answer('⚠️ Нельзя назначить себя администратором')
        else:
            target_in_network = await execute_query('SELECT userid FROM stats WHERE userid = ? AND network = ?', 
                                            (target_user, user_data[1]))
            if target_in_network:
                admins_result = await execute_query('SELECT admins FROM networks WHERE owner_id = ?', 
                                                 (user_data[1],))
                if admins_result:
                    admins = parse_array(admins_result[0][0])
                    if target_user in admins:
                        await message.answer('⚠️ Этот пользователь уже является администратором')
                    else:
                        await message.answer('✅ Вы успешно назначили клуб администратором')
                        new_admins = admins
                        new_admins.append(target_user)
                        await execute_update('UPDATE networks SET admins = ? WHERE owner_id = ?', 
                                     (format_array(new_admins), user_data[1]))
            else:
                await message.answer('❌ Вы не являетесь владельцем франшизы или этот пользователь не найден в ней')
    else:
        await message.answer('⚠️ Используйте: /set_admin (ID пользователя)')

@cmd_franchise_router.message(Command('delete_admin'))
async def cmd_delete_admin(message: Message):
    user = await execute_query_one('SELECT name, network FROM stats WHERE userid = ?', (message.from_user.id,))
    if not user:
        await message.answer('Сначала зарегистрируйтесь - /start')
        return
        
    await update_data(message.from_user.username, message.from_user.id)
    await add_action(message.from_user.id, 'cmd_delete_admin')
    
    user_data = user
    if len(message.text.split()) > 1 and message.text.split()[1].isdigit():
        target_user = int(message.text.split()[1])
        
        if target_user == message.from_user.id:
            await message.answer('⚠️ Нельзя снять себя с должности администратора')
        else:
            target_in_network = await execute_query('SELECT userid FROM stats WHERE userid = ? AND network = ?', 
                                            (target_user, user_data[1]))
            if target_in_network:
                await message.answer('✅ Вы успешно сняли клуб с должности администратора')
                admins_result = await execute_query('SELECT admins FROM networks WHERE owner_id = ?', 
                                                 (user_data[1],))
                if admins_result:
                    admins = parse_array(admins_result[0][0])
                    new_admins = [admin for admin in admins if admin != target_user]
                    await execute_update('UPDATE networks SET admins = ? WHERE owner_id = ?', 
                                 (format_array(new_admins), user_data[1]))
            else:
                await message.answer('❌ Вы не являетесь владельцем франшизы или этот пользователь не найден в ней')
    else:
        await message.answer('⚠️ Используйте: /delete_admin (ID пользователя)')

@cmd_franchise_router.message(Command('delete_user'))
async def cmd_delete_user(message: Message):
    user = await execute_query_one('SELECT name, network FROM stats WHERE userid = ?', (message.from_user.id,))
    if not user:
        await message.answer('Сначала зарегистрируйтесь - /start')
        return
        
    await update_data(message.from_user.username, message.from_user.id)
    await add_action(message.from_user.id, 'cmd_delete_user')
    
    user_data = user
    if len(message.text.split()) > 1 and message.text.split()[1].isdigit():
        target_user = int(message.text.split()[1])
        
        if target_user == message.from_user.id:
            await message.answer('⚠️ Нельзя удалить себя')
        else:
            admins_result = await execute_query('SELECT admins FROM networks WHERE owner_id = ?', 
                                             (user_data[1],))
            member = await execute_query('SELECT userid FROM stats WHERE userid = ? AND network = ?', 
                                 (target_user, user_data[1]))
            
            if member and member[0][0] == user_data[1]:
                await message.answer('❌ Нельзя удалить владельца')
            elif admins_result and message.from_user.id in parse_array(admins_result[0][0]) and member and member[0][0] in parse_array(admins_result[0][0]):
                await message.answer('❌ Нельзя удалить администратора')
            elif member:
                if message.from_user.id == user_data[1] or (admins_result and message.from_user.id in parse_array(admins_result[0][0])):
                    await message.answer('✅ Вы успешно исключили клуб из франшизы')
                    await bot.send_message(target_user, '🫷 Ваш клуб был исключен из франшизы')
                    await execute_update('UPDATE stats SET network = NULL WHERE userid = ?', (target_user,))
                    
                    if admins_result and target_user in parse_array(admins_result[0][0]):
                        admins = parse_array(admins_result[0][0])
                        new_admins = [admin for admin in admins if admin != target_user]
                        await execute_update('UPDATE networks SET admins = ? WHERE owner_id = ?', 
                                     (format_array(new_admins), user_data[1]))
                else:
                    await message.answer('❌ Вы не являетесь владельцем франшизы или её администратором')
            else:
                await message.answer('❌ Этот пользователь не найден в франшизе')
    else:
        await message.answer('⚠️ Используйте: /delete_user (ID пользователя)')

@cmd_franchise_router.message(Command('ban_user'))
async def cmd_ban_user(message: Message):
    user = await execute_query_one('SELECT name, network FROM stats WHERE userid = ?', (message.from_user.id,))
    if not user:
        await message.answer('Сначала зарегистрируйтесь - /start')
        return
        
    await update_data(message.from_user.username, message.from_user.id)
    await add_action(message.from_user.id, 'cmd_ban_user')
    
    user_data = user
    if len(message.text.split()) > 1 and message.text.split()[1].isdigit():
        target_user = int(message.text.split()[1])
        
        if target_user == message.from_user.id:
            await message.answer('⚠️ Нельзя забанить себя')
        else:
            admins_result = await execute_query('SELECT admins FROM networks WHERE owner_id = ?', 
                                             (user_data[1],))
            member = await execute_query('SELECT userid FROM stats WHERE userid = ? AND network = ?', 
                                 (target_user, user_data[1]))
            
            if member and member[0][0] == user_data[1]:
                await message.answer('❌ Нельзя забанить владельца')
            elif admins_result and message.from_user.id in parse_array(admins_result[0][0]) and member and member[0][0] in parse_array(admins_result[0][0]):
                await message.answer('❌ Нельзя забанить администратора')
            elif member:
                if message.from_user.id == user_data[1] or (admins_result and message.from_user.id in parse_array(admins_result[0][0])):
                    await message.answer('✅ Вы успешно заблокировали доступ к франшизе этому клубу')
                    await execute_update('UPDATE stats SET network = NULL WHERE userid = ?', (target_user,))
                    
                    ban_users_result = await execute_query('SELECT ban_users FROM networks WHERE owner_id = ?', 
                                                        (user_data[1],))
                    if ban_users_result:
                        ban_users = parse_array(ban_users_result[0][0])
                        new_ban_users = ban_users
                        new_ban_users.append(target_user)
                        await execute_update('UPDATE networks SET ban_users = ? WHERE owner_id = ?', 
                                     (format_array(new_ban_users), user_data[1]))
                    
                    if admins_result and target_user in parse_array(admins_result[0][0]):
                        admins = parse_array(admins_result[0][0])
                        new_admins = [admin for admin in admins if admin != target_user]
                        await execute_update('UPDATE networks SET admins = ? WHERE owner_id = ?', 
                                     (format_array(new_admins), user_data[1]))
                else:
                    await message.answer('❌ Вы не являетесь владельцем франшизы или её администратором')
            else:
                await message.answer('❌ Этот пользователь не находится в франшизе')
    else:
        await message.answer('⚠️ Используйте: /ban_user (ID пользователя)')

@cmd_franchise_router.message(Command('reban_user'))
async def cmd_reban_user(message: Message):
    user = await execute_query_one('SELECT name, network FROM stats WHERE userid = ?', (message.from_user.id,))
    if not user:
        await message.answer('Сначала зарегистрируйтесь - /start')
        return
        
    await update_data(message.from_user.username, message.from_user.id)
    await add_action(message.from_user.id, 'cmd_reban_user')
    
    user_data = user
    if len(message.text.split()) > 1 and message.text.split()[1].isdigit():
        target_user = int(message.text.split()[1])
        
        if target_user == message.from_user.id:
            await message.answer('⚠️ Нельзя разбанить себя')
        else:
            admins_result = await execute_query('SELECT admins FROM networks WHERE owner_id = ?', 
                                             (user_data[1],))
            ban_users_result = await execute_query('SELECT ban_users FROM networks WHERE owner_id = ?', 
                                                (user_data[1],))
            
            if target_user == user_data[1]:
                await message.answer('❌ Нельзя разбанить владельца')
            elif admins_result and message.from_user.id in parse_array(admins_result[0][0]) and target_user in parse_array(admins_result[0][0]):
                await message.answer('❌ Нельзя разбанить администратора')
            elif ban_users_result and target_user in parse_array(ban_users_result[0][0]):
                if message.from_user.id == user_data[1] or (admins_result and message.from_user.id in parse_array(admins_result[0][0])):
                    await message.answer('✅ Вы успешно разблокировали доступ к франшизе этому клубу')
                    ban_users = parse_array(ban_users_result[0][0])
                    new_ban_users = [user_id for user_id in ban_users if user_id != target_user]
                    await execute_update('UPDATE networks SET ban_users = ? WHERE owner_id = ?', 
                                 (format_array(new_ban_users), user_data[1]))
                else:
                    await message.answer('❌ Вы не являетесь владельцем франшизы или её администратором')
            else:
                await message.answer('❌ Этот пользователь не найден в бане')
    else:
        await message.answer('⚠️ Используйте: /reban_user (ID пользователя)')

@cmd_franchise_router.message(Command('franchise'))
async def cmd_franchise(message: Message):
    user = await execute_query_one('SELECT name, network FROM stats WHERE userid = ?', (message.from_user.id,))
    if not user:
        await message.answer('Сначала зарегистрируйтесь - /start')
        return
        
    await update_data(message.from_user.username, message.from_user.id)
    await add_action(message.from_user.id, 'cmd_franchise')
    
    user_data = user
    if user_data[1] is None:
        markup = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text='🆕 Создать новую франшизу', callback_data=f'network_create_{message.from_user.id}')],
            [InlineKeyboardButton(text='🤝 Вступить в франшизу', callback_data=f'network_search_{message.from_user.id}')]
        ])
        await message.answer('🌐 Вы не состоите в франшизе', reply_markup=markup)
    else:
        network = await execute_query('SELECT name, owner_id, description, income, type, admins FROM networks WHERE owner_id = ?', 
                               (user_data[1],))
        
        if network:
            network = network[0]
            admins = parse_array(network[5])
            is_owner = network[1] == message.from_user.id
            is_admin = message.from_user.id in admins
            
            if network[4] == 'request':
                markup1 = InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text='✏️ Изменить франшизу', callback_data=f'network_edit_{message.from_user.id}')],
                    [InlineKeyboardButton(text='👥 Участники', callback_data=f'network_members_1_{message.from_user.id}')],
                    [InlineKeyboardButton(text='📫 Заявки', callback_data=f'network_requests_{message.from_user.id}')],
                    [InlineKeyboardButton(text='📤 Сделать рассылку', callback_data=f'network_mailing_{message.from_user.id}')]
                ])
            else:
                markup1 = InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text='✏️ Изменить франшизу', callback_data=f'network_edit_{message.from_user.id}')],
                    [InlineKeyboardButton(text='👥 Участники', callback_data=f'network_members_1_{message.from_user.id}')],
                    [InlineKeyboardButton(text='📤 Сделать рассылку', callback_data=f'network_mailing_{message.from_user.id}')]
                ])
            
            if is_owner:
                markup1.inline_keyboard.extend([
                    [InlineKeyboardButton(text='🔄️ Передать права на франшизу', callback_data=f'network_owner_{message.from_user.id}')],
                    [InlineKeyboardButton(text='🗑️ Удалить франшизу', callback_data=f'network_delete_{message.from_user.id}')]
                ])
            else:
                markup1.inline_keyboard.append([InlineKeyboardButton(text='↩️ Покинуть франшизу', callback_data=f'network_left_{message.from_user.id}')])
            
            net_type = ''
            if network[4] == 'open':
                net_type = 'Открытая'
            elif network[4] == 'close':
                net_type = 'Закрытая'
            elif network[4] == 'request':
                net_type = 'По заявке'
            
            members = await execute_query('SELECT COUNT(*) FROM stats WHERE network = ?', (network[1],))
            
            if is_owner or is_admin:
                await message.answer(
                    f'🌐 Франшиза {network[0]}\n\n'
                    f'🆔 ID: {network[1]}\n'
                    f'💭 Описание: {network[2]}\n'
                    f'🔘 Статус: {net_type}\n\n'
                    f'👥 Количество клубов-участников: {members[0][0]}\n\n'
                    f'💰 Заработано за эту неделю: {network[3]}$\n'
                    f'🏆 Топ франшизы: /franchise_info', 
                    reply_markup=markup1
                )
            else:
                markup2 = InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text='↩️ Покинуть франшизу', callback_data=f'network_left_{message.from_user.id}')]
                ])
                await message.answer(
                    f'🌐 Франшиза {network[0]}\n\n'
                    f'🆔 ID: {network[1]}\n'
                    f'💭 Описание: {network[2]}\n'
                    f'🔘 Статус: {net_type}\n\n'
                    f'👥 Количество клубов-участников: {members[0][0]}\n\n'
                    f'💰 Заработано за эту неделю: {network[3]}$\n'
                    f'🏆 Топ франшизы: /franchise_info', 
                    reply_markup=markup2
                )


@cmd_franchise_router.message(Command('franchise_info'))
async def cmd_franchise_info(message: Message):
    user = await execute_query_one('SELECT name, network FROM stats WHERE userid = ?', (message.from_user.id,))
    if not user:
        await message.answer('Сначала зарегистрируйтесь - /start')
        return
        
    await update_data(message.from_user.username, message.from_user.id)
    await add_action(message.from_user.id, 'cmd_franchise_info')
    
    user_data = user
    info = await execute_query('SELECT name, net_inc FROM stats WHERE network = ? ORDER BY net_inc DESC LIMIT 10', 
                        (user_data[1],))
    
    text = '💸 Топ 10 игроков твоей франшизы по заработанным $ за неделю:'
    num = 1
    for user_info in info:
        text += f'\n{num}) {user_info[0]} - {format_number_short(user_info[1], True)}$'
        num += 1
    
    await message.answer(text)

# ===== ECONOMY HANDLERS =====
@cmd_economy_router.message(F.text == '🛒 Магазин')
async def msg_shop(message: Message):
    await cmd_shop(message)

@cmd_economy_router.message(Command('taxes'))
async def cmd_taxes(message: Message):
    user = await execute_query_one('SELECT name, taxes, room FROM stats WHERE userid = ?', (message.from_user.id,))
    if not user:
        await message.answer('Сначала зарегистрируйтесь - /start')
        return
        
    await update_data(message.from_user.username, message.from_user.id)
    await add_action(message.from_user.id, 'cmd_taxes')
    
    user_data = user
    max_taxes = 0
    for tax in taxes:
        if user_data[2] == tax[0]:
            max_taxes = tax[1]
            break
    
    await message.answer(
        f'👮‍♂️ <b>Меню налогов</b>\n\n'
        f'Ваш налог: <b>{format_number_short(user_data[1], True)}$ / {format_number_short(max_taxes, True)}$</b>\n\n'
        f'❗Если налоги достигнут максимума, то ваш доход будет заморожен!\n\n'
        f'Уплатить налоги: /pay_taxes',
        parse_mode='HTML'
    )
        
@cmd_user_router.message(Command('pay_taxes'))
async def cmd_pay_taxes(message: Message):
    # Проверяем бустер автоматизации
    user_boosters = await execute_query_one(
        'SELECT auto_booster_end FROM stats WHERE userid = ?',
        (message.from_user.id,)
    )
    
    if user_boosters and user_boosters[0]:
        auto_booster_end = safe_parse_datetime(user_boosters[0])
        if auto_booster_end and auto_booster_end > datetime.datetime.now():
            await message.answer(
                '💰 <b>Налоги оплачиваются автоматически!</b>\n\n'
                'У вас активен бустер автоматизации. Система автоматически оплачивает налоги за вас каждый час.\n\n'
                'Чтобы оплачивать налоги вручную, дождитесь окончания бустера.',
                parse_mode='HTML'
            )
            return
    
    user = await execute_query_one('SELECT name, taxes, bal FROM stats WHERE userid = ?', (message.from_user.id,))
    if not user:
        await message.answer('Сначала зарегистрируйтесь - /start')
        return
        
    await update_data(message.from_user.username, message.from_user.id)
    await add_action(message.from_user.id, 'cmd_pay_taxes')
    
    user_data = user
    if user_data[2] >= user_data[1]:
        await execute_update('UPDATE stats SET bal = bal - ?, taxes = 0 WHERE userid = ?', (user_data[1], message.from_user.id))
        await message.answer(f'✅ Вы успешно уплатили все налоги. Общая сумма составила {format_number_short(user_data[1], True)}$')
    else:
        await message.answer('❌ У вас недостаточно средств')
        
        
@cmd_economy_router.message(Command('shop'))
async def cmd_shop(message: Message):
    user = await execute_query_one('SELECT name FROM stats WHERE userid = ?', (message.from_user.id,))
    if not user:
        await message.answer('Сначала зарегистрируйтесь - /start')
        return
        
    await update_data(message.from_user.username, message.from_user.id)
    await add_action(message.from_user.id, 'cmd_shop')
    
    markup = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text='🖥 Компьютеры', callback_data=f'shop_pc_{message.from_user.id}')],
        [InlineKeyboardButton(text='⏫ Комната', callback_data=f'shop_room_{message.from_user.id}')],
        [InlineKeyboardButton(text='🔧 Улучшения', callback_data=f'shop_upgrade_{message.from_user.id}')],
        [InlineKeyboardButton(text='📢 Реклама', callback_data=f'shop_ads_{message.from_user.id}')]
    ])
    
    await message.answer('🛒 PC Club Shop\nВыберите раздел:', reply_markup=markup)

@cmd_economy_router.message(F.text[:6] == '/sell_')
async def cmd_sell(message: Message):
    user = await execute_query_one('SELECT name, bal, income, pc FROM stats WHERE userid = ?', (message.from_user.id,))
    if not user:
        await message.answer('❌ Сначала нужно зарегистрироваться - используйте команду /start')
        return
        
    await update_data(message.from_user.username, message.from_user.id)
    await add_action(message.from_user.id, 'cmd_sell')
    
    # Обрабатываем команду вида "/sell_1 5" или "/sell_1@username 5"
    text_parts = message.text[6:].strip().split()
    if len(text_parts) == 0:
        await message.answer('ℹ️ Формат команды: /sell_<уровень> <количество>\n'
                           'Пример: /sell_1 5 - продать 5 компьютеров 1 уровня')
        return
    
    # Первая часть - уровень и возможно юзернейм
    level_part = text_parts[0].split('@')[0]  # Берем только уровень, игнорируя @username
    quantity = '1'  # Значение по умолчанию
    
    # Если есть вторая часть - это количество
    if len(text_parts) > 1:
        quantity = text_parts[1].split('@')[0]  # Берем количество, игнорируя @username
    
    if quantity == 'max':
        # Получаем все компьютеры этого уровня у пользователя
        pcs_count = await execute_query_one(
            'SELECT COUNT(*) FROM pc WHERE userid = ? AND lvl = ?',
            (message.from_user.id, int(level_part))
        )
        quantity = str(pcs_count[0] if pcs_count else 0)
    
    if level_part.isdigit() and quantity.isdigit():
        level = int(level_part)
        quantity = int(quantity)
        
        if quantity <= 0:
            await message.answer('❌ Укажите количество больше нуля')
            return
            
        # Получаем полный список всех ПК для поиска цены
        expansion_level = await get_expansion_level(message.from_user.id)
        all_prices = prices.copy()
        
        # Добавляем ПК из экспансий
        for expansion in range(1, expansion_level + 1):
            expansion_pcs = get_prices_for_expansion(expansion)
            all_prices.extend(expansion_pcs)
        
        # Ищем ПК в полном списке
        pc_found = False
        pc_data = None
        
        for pc_item in all_prices:
            if level == pc_item[0]:
                pc_found = True
                pc_data = pc_item
                break
        
        if not pc_found:
            await message.answer('❌ Компьютер такого уровня не найден')
            return
            
        pcs = await execute_query('SELECT id FROM pc WHERE userid = ? AND lvl = ? LIMIT ?', 
                           (message.from_user.id, level, quantity))
        
        if len(pcs) >= quantity:
            total_income = 0
            pc_ids = [pc[0] for pc in pcs]
            
            # Удаляем компьютеры одним запросом
            await execute_update('DELETE FROM pc WHERE id IN (' + ','.join('?'*len(pc_ids)) + ')', pc_ids)
            
            # Рассчитываем сумму возврата (50% от исходной цены)
            total_income = pc_data[2] // 2 * quantity
            
            # Получаем доход от одного такого компьютера
            pc_income = Decimal(str(pc_data[1]))
            
            await execute_update('UPDATE stats SET bal = bal + ?, income = income - ?, pc = pc - ? WHERE userid = ?', 
                         (total_income, float(pc_income * quantity), quantity, message.from_user.id))
            
            await message.answer(f'💻 Вы успешно продали {quantity} шт. | Компьютер {level} ур. | 💰 +{total_income}$')
        else:
            available = await execute_query_one('SELECT COUNT(*) FROM pc WHERE userid = ? AND lvl = ?', 
                                        (message.from_user.id, level))
            await message.answer(f'❌ Недостаточно компьютеров {level} уровня для продажи\n'
                                f'📊 Доступно: {available[0]} шт.')
    else:
        await message.answer('ℹ️ Формат команды: /sell_<уровень> <количество>\n'
                           'Пример: /sell_1 5 - продать 5 компьютеров 1 уровня\n'
                           'Или: /sell_1 max - продать все компьютеры 1 уровня')


@cmd_economy_router.message(F.text.startswith('/buy_'))
async def cmd_buy(message: Message):
    # Проверка кулдауна
    user_id = message.from_user.id
    current_time = time.time()

    if user_id in buy_cooldowns:
        time_passed = current_time - buy_cooldowns[user_id]
        if time_passed < BUY_COOLDOWN:
            remaining = BUY_COOLDOWN - time_passed
            await message.answer(f'⏳ Подождите {remaining:.1f} сек. перед следующей покупкой')
            return

    user = await execute_query_one('SELECT name, bal, room, pc, income FROM stats WHERE userid = ?', (user_id,))
    if not user:
        await message.answer('❌ Сначала нужно зарегистрироваться - используйте команду /start')
        return

    await update_data(message.from_user.username, user_id)
    await add_action(user_id, 'cmd_buy')
    
    user_data = user
    text_parts = message.text[5:].strip().split()
    if len(text_parts) == 0:
        await message.answer('ℹ️ Формат команды: /buy_<уровень> <количество>\n'
                           'Пример: /buy_1 5 - купить 5 компьютеров 1 уровня')
        return
    
    level_part = text_parts[0].split('@')[0]
    quantity = '1'
    
    if len(text_parts) > 1:
        quantity = text_parts[1].split('@')[0]
    
    if quantity == 'max':
        max_pcs = user_data[2] * 5 - user_data[3]
        # Находим ПК во всех доступных (включая экспансии)
        available_pcs = await get_available_pcs(message.from_user.id)
        pc_found = None
        for pc in available_pcs:
            if pc[0] == int(level_part):
                pc_found = pc
                break
        
        if pc_found:
            while user_data[1] < pc_found[2] * max_pcs and max_pcs > 0:
                max_pcs -= 1
            quantity = str(max_pcs)
        else:
            quantity = '0'
    
    if level_part.isdigit() and quantity.isdigit():
        level = int(level_part)
        quantity = int(quantity)
        
        if quantity <= 0:
            await message.answer('❌ Укажите количество больше нуля')
            return
            
        # Получаем полный список доступных ПК
        available_pcs = await get_available_pcs(message.from_user.id)
        pc_found = None
        
        for pc in available_pcs:
            if pc[0] == level:
                pc_found = pc
                break
        
        if not pc_found:
            await message.answer('❌ Компьютер такого уровня не найден или вам не доступен!')
            return
            
        # Проверяем, что уровень компьютера не превышает уровень комнаты
        if user_data[2] < level:
            await message.answer(f'❌ Компьютер уровня {level} вам не доступен! Требуется уровень комнаты {level}')
            return
            
        if user_data[1] >= pc_found[2] * quantity and user_data[3] + quantity <= user_data[2] * 5:
            # Добавляем репутацию за покупку ПК (1 очко за каждый компьютер)
            rep_points = quantity
            new_points, new_level, level_up = await add_reputation(
                message.from_user.id, rep_points, "buy_pc"
            )
            
            pc_income = Decimal(str(pc_found[1]))
            await execute_update('UPDATE stats SET bal = bal - ?, pc = pc + ?, income = income + ?, all_pcs = all_pcs + ? WHERE userid = ?', 
                         (pc_found[2] * quantity, quantity, float(pc_income * quantity), quantity, message.from_user.id))
            
            for _ in range(quantity):
                await execute_update('INSERT INTO pc (userid, lvl, income) VALUES (?, ?, ?)', 
                             (message.from_user.id, level, float(pc_income)))
            
            # Новое сообщение с репутацией
            response_text = (
                f'💻 Вы успешно купили {quantity} шт. | Компьютер {level} ур. |\n'
                f'💰Затраты: -{format_number_short(pc_found[2] * quantity, True)}$\n'
                f'✨ +{rep_points} Репутации'
            )

            if level_up:
                rep_info = await get_current_reputation_info(message.from_user.id)
                response_text += f"\n\n🎉 Новый уровень репутации: {rep_info['level_name']}!"

            await message.answer(response_text)

            # Обновляем время последней покупки
            buy_cooldowns[user_id] = time.time()
            
        elif user_data[1] < pc_found[2] * quantity:
            await message.answer('❌ Недостаточно средств для покупки!')
        elif user_data[3] + quantity > user_data[2] * 5:
            await message.answer('❌ Не хватает места в комнате! Улучшите комнату для увеличения вместимости.')
    else:
        await message.answer('ℹ️ Формат команды: /buy_<уровень> <количество>\n'
                           'Пример: /buy_1 5 - купить 5 компьютеров 1 уровня\n'
                           'Или: /buy_1 max - купить максимально возможное количество')        
# ===== ADMIN HANDLERS =====
@cmd_admin_router.message(Command('ad'))
async def cmd_ad(message: Message):
    if message.from_user.id not in ADMIN:
        await message.answer('❌ Недостаточно прав')
        return
    
    # Проверяем, что команда отправлена в ответ на сообщение
    if not message.reply_to_message:
        await message.answer('❌ Команда должна быть отправлена в ответ на сообщение, которое нужно разослать')
        return
    
    # Получаем сообщение для рассылки
    original_message = message.reply_to_message
    
    # Создаем предпросмотр - копируем сообщение
    try:
        # Пытаемся скопировать сообщение с тем же форматированием
        if original_message.text:
            preview_text = f"📢 <b>Предпросмотр рассылки:</b>\n\n{original_message.text}"
            sent_preview = await message.answer(preview_text, parse_mode='HTML')
        elif original_message.caption:
            preview_text = f"📢 <b>Предпросмотр рассылки:</b>\n\n{original_message.caption}"
            if original_message.photo:
                sent_preview = await message.answer_photo(
                    photo=original_message.photo[-1].file_id,
                    caption=preview_text,
                    parse_mode='HTML'
                )
            elif original_message.video:
                sent_preview = await message.answer_video(
                    video=original_message.video.file_id,
                    caption=preview_text,
                    parse_mode='HTML'
                )
            else:
                sent_preview = await message.answer(preview_text, parse_mode='HTML')
        else:
            await message.answer('❌ Неподдерживаемый тип сообщения для рассылки')
            return
    
    except Exception as e:
        await message.answer(f'❌ Ошибка при создании предпросмотра: {e}')
        return
    
    # Создаем клавиатуру с кнопками подтверждения
    markup = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text='✅ Да, разослать', callback_data=f'ad_confirm_{message.from_user.id}'),
            InlineKeyboardButton(text='❌ Нет, отменить', callback_data=f'ad_cancel_{message.from_user.id}')
        ]
    ])
    
    # Сохраняем информацию о сообщении для рассылки в callback data
    # Для простоты сохраняем message_id оригинального сообщения
    await message.answer(
        '❓ Вы уверены, что хотите разослать это сообщение всем пользователям?',
        reply_markup=markup
    )
    
    # Сохраняем информацию о сообщениях для последующего использования
    # Можно использовать временное хранилище или добавить в базу данных
    # Для простоты используем глобальный словарь (в продакшене лучше использовать БД)
    if not hasattr(bot, 'pending_ads'):
        bot.pending_ads = {}
    
    bot.pending_ads[f'{message.from_user.id}'] = {
        'original_message_id': original_message.message_id,
        'chat_id': original_message.chat.id,
        'preview_message_id': sent_preview.message_id
    }

# Обработчик подтверждения рассылки
@cb_admin_router.callback_query(F.data.startswith('ad_confirm_'))
async def cb_ad_confirm(callback: CallbackQuery):
    userid = callback.data.split('_')[-1]
    
    if not userid.isdigit() or callback.from_user.id != int(userid):
        await callback.answer('⚠️ Это не твое сообщение', show_alert=True)
        return
        
    if callback.from_user.id not in ADMIN:
        await callback.answer('❌ Недостаточно прав', show_alert=True)
        return
    
    # Получаем сохраненную информацию о сообщении
    if not hasattr(bot, 'pending_ads') or f'{callback.from_user.id}' not in bot.pending_ads:
        await callback.answer('❌ Информация о рассылке устарела', show_alert=True)
        return
    
    ad_info = bot.pending_ads[f'{callback.from_user.id}']
    
    try:
        # Получаем оригинальное сообщение
        original_message = await bot.forward_message(
            chat_id=callback.message.chat.id,
            from_chat_id=ad_info['chat_id'],
            message_id=ad_info['original_message_id']
        )
        
        # Начинаем рассылку
        await callback.message.edit_text('🔄 Начинаю рассылку...')
        
        users = await execute_query('SELECT userid FROM stats')
        total_users = len(users)
        successful = 0
        failed = 0
        blocked = 0
        not_found = 0
        
        progress_msg = await callback.message.answer(f'📊 Прогресс: 0/{total_users}')
        
        for i, user in enumerate(users, 1):
            user_id = user[0]
            
            # Обновляем прогресс каждые 50 пользователей
            if i % 50 == 0 or i == total_users:
                try:
                    await progress_msg.edit_text(
                        f'📊 Прогресс: {i}/{total_users}\n'
                        f'✅ Успешно: {successful}\n'
                        f'❌ Ошибок: {failed}'
                    )
                except:
                    pass
            
            try:
                # Пытаемся отправить сообщение
                if original_message.text:
                    await bot.send_message(user_id, original_message.text)
                elif original_message.photo:
                    await bot.send_photo(
                        user_id, 
                        photo=original_message.photo[-1].file_id,
                        caption=original_message.caption
                    )
                elif original_message.video:
                    await bot.send_video(
                        user_id,
                        video=original_message.video.file_id,
                        caption=original_message.caption
                    )
                else:
                    # Для других типов сообщений используем forward
                    await bot.forward_message(
                        chat_id=user_id,
                        from_chat_id=ad_info['chat_id'],
                        message_id=ad_info['original_message_id']
                    )
                
                successful += 1
                
            except TelegramForbiddenError as e:
                if "user is deactivated" in str(e):
                    await execute_update('DELETE FROM stats WHERE userid = ?', (user_id,))
                    blocked += 1
                elif "bot was blocked" in str(e):
                    blocked += 1
                else:
                    failed += 1
            except TelegramBadRequest as e:
                if "chat not found" in str(e).lower():
                    not_found += 1
                else:
                    failed += 1
            except Exception as e:
                failed += 1
        
        # Итоговый отчет
        report = (
            f'✅ <b>Рассылка завершена!</b>\n\n'
            f'📊 <b>Результаты:</b>\n'
            f'• Всего пользователей: {total_users}\n'
            f'• ✅ Успешно: {successful}\n'
            f'• 🔒 Заблокировали бота: {blocked}\n'
            f'• ❌ Не найдены: {not_found}\n'
            f'• ⚠️ Ошибок: {failed}\n\n'
            f'⏰ Время: {datetime.datetime.now().strftime("%d.%m.%Y %H:%M")}'
        )
        
        await callback.message.edit_text(report, parse_mode='HTML')
        await progress_msg.delete()
        
        # Удаляем предпросмотр
        try:
            await bot.delete_message(
                chat_id=callback.message.chat.id,
                message_id=ad_info['preview_message_id']
            )
        except:
            pass
        
        # Очищаем временные данные
        del bot.pending_ads[f'{callback.from_user.id}']
        
    except Exception as e:
        await callback.message.edit_text(f'❌ Ошибка при рассылке: {e}')
        logger.error(f"Error in ad distribution: {e}")

# Обработчик отмены рассылки
@cb_admin_router.callback_query(F.data.startswith('ad_cancel_'))
async def cb_ad_cancel(callback: CallbackQuery):
    userid = callback.data.split('_')[-1]
    
    if not userid.isdigit() or callback.from_user.id != int(userid):
        await callback.answer('⚠️ Это не твое сообщение', show_alert=True)
        return
    
    # Получаем сохраненную информацию о сообщении
    if hasattr(bot, 'pending_ads') and f'{callback.from_user.id}' in bot.pending_ads:
        ad_info = bot.pending_ads[f'{callback.from_user.id}']
        
        # Удаляем предпросмотр
        try:
            await bot.delete_message(
                chat_id=callback.message.chat.id,
                message_id=ad_info['preview_message_id']
            )
        except:
            pass
        
        # Очищаем временные данные
        del bot.pending_ads[f'{callback.from_user.id}']
    
    await callback.message.edit_text('❌ Рассылка отменена')
    await callback.answer('Рассылка отменена')

@cmd_admin_router.message(Command('active'))
async def cmd_active(message: Message):
    if message.from_user.id in ADMIN:
        text_parts = message.text.split(' ')
        if len(text_parts) > 1 and text_parts[1].isdigit():
            days = int(text_parts[1])
            active = await execute_query('SELECT userid FROM actions WHERE dt >= ?',
                                  (datetime.datetime.now() - datetime.timedelta(days=days),))
            users = len({el[0] for el in active})
            await message.answer(f'Активные пользователи за последние {days} дней: {users}')
        else:
            await message.answer('⚠️ Используйте: /active (количество дней)')

@cmd_admin_router.message(Command('add_promo'))
async def cmd_add_promo(message: Message):
    if message.from_user.id in ADMIN:
        text_parts = message.text.split(' ')
        if len(text_parts) >= 4:
            use_max = int(text_parts[1])
            reward_type = text_parts[2]
            quantity = int(text_parts[3])

            alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
            promo = ''.join(random.choice(alphabet) for _ in range(10))

            await execute_update('INSERT INTO promos (name, use_max, reward, quantity) VALUES (?, ?, ?, ?)',
                         (promo, use_max, reward_type, quantity))

            await message.answer(f'Промокод создан: `{promo}`', parse_mode='Markdown')
        else:
            await message.answer('⚠️ Используйте: /add_promo (use_max) (reward_type) (quantity)')

@cmd_admin_router.message(Command('stat'))
async def cmd_stat(message: Message):
    if message.from_user.id in ADMIN:
        text_parts = message.text.split(' ')
        if len(text_parts) > 1 and text_parts[1].isdigit():
            user_id = int(text_parts[1])
            stats = await execute_query('SELECT * FROM stats WHERE userid = ?', (user_id,))

            if stats:
                stats = stats[0]
                text = (
                    f'Статистика пользователя:\n'
                    f'Ник: {stats[9]}\n'
                    f'Юзернейм: {stats[8]}\n'
                    f'Баланс: {stats[1]}\n'
                    f'Ур. комнаты: {stats[2]}\n'
                    f'Количество компьютеров: {stats[3]}\n'
                    f'Доход: {stats[5]}\n'
                    f'Зарегистрирован: {stats[6]}\n'
                    f'Сеть: {stats[7]}\n'
                    f'Весь доход: {stats[10]}\n'
                    f'Премиум до {stats[11]}\n'
                    f'Реферал: {stats[12]}'
                )
                await bot.send_message(message.from_user.id, text)
            else:
                await message.answer('❌ Пользователь не найден')
        else:
            await message.answer('⚠️ Используйте: /stat (ID пользователя)')

@cmd_admin_router.message(Command('stat_network'))
async def cmd_stat_network(message: Message):
    if message.from_user.id in ADMIN:
        text_parts = message.text.split(' ')
        if len(text_parts) > 1 and text_parts[1].isdigit():
            network_id = int(text_parts[1])
            stats = await execute_query('SELECT * FROM networks WHERE owner_id = ?', (network_id,))

            if stats:
                stats = stats[0]
                text = (
                    f'Статистика франшизы:\n'
                    f'Название: {stats[0]}\n'
                    f'Описание: {stats[2]}\n'
                    f'Заработок за неделю: {stats[3]}'
                )
                await bot.send_message(message.from_user.id, text)
            else:
                await message.answer('❌ Франшиза не найдена')
        else:
            await message.answer('⚠️ Используйте: /stat_network (ID франшизы)')

@cmd_admin_router.message(Command('botstats'))
async def cmd_bot_info(message: Message):
    if message.from_user.id not in ADMIN:
        await message.answer('❌ Недостаточно прав')
        return
    
    try:
        # Базовая статистика
        stats = await execute_query('SELECT COUNT(*) FROM stats')
        networks = await execute_query('SELECT COUNT(*) FROM networks')
        active = await execute_query('SELECT userid FROM actions WHERE dt >= ?', 
                              (datetime.datetime.now() - datetime.timedelta(days=3),))
        not_bots = await execute_query('SELECT COUNT(*) FROM stats WHERE bal != 1000')
        users = len({el[0] for el in active})
        
        # Расширенная статистика пользователей
        premium_users = await execute_query('SELECT COUNT(*) FROM stats WHERE premium > ?', 
                                     (datetime.datetime.now(),))
        
        # Экономика - общий баланс всех пользователей
        total_usd = await execute_query('SELECT SUM(bal) FROM stats WHERE bal > 0')
        total_income = await execute_query('SELECT SUM(income) FROM stats WHERE income > 0')
        
        # Время работы бота (нужно сохранять время старта при запуске)
        # Добавим глобальную переменную для времени старта
        if not hasattr(bot, 'start_time'):
            bot.start_time = datetime.datetime.now()
        
        uptime = datetime.datetime.now() - bot.start_time
        days = uptime.days
        hours = uptime.seconds // 3600
        minutes = (uptime.seconds % 3600) // 60
        
        # Пинг (примерное время выполнения запроса)
        ping_start = datetime.datetime.now()
        await execute_query('SELECT 1')
        ping_end = datetime.datetime.now()
        ping_ms = int((ping_end - ping_start).total_seconds() * 1000)
        
        # Версия и последний рестарт
        version = "2.1.3"
        last_restart = bot.start_time.strftime("%d.%m.%Y %H:%M")
        
        # Форматируем числа для читаемости
        def format_large_number(number):
            if number is None:
                return "0"
            return f"{number:,}".replace(',', '.')
        
        total_usd_amount = total_usd[0][0] if total_usd and total_usd[0][0] else 0
        total_income_amount = total_income[0][0] if total_income and total_income[0][0] else 0
        
        # Создаем красивый вывод
        response = (
            "🤖 <b>Расширенная статистика бота</b>\n\n"
            
            "👥 <b>Пользователи:</b>\n"
            f"▸ Всего: <code>{format_large_number(stats[0][0])}</code>\n"
            f"▸ Активных: <code>{format_large_number(users)}</code>\n"
            f"▸ Premium: <code>{format_large_number(premium_users[0][0])}</code>\n\n"
            
            "💰 <b>Экономика:</b>\n"
            f"▸ Всего USD: <code>${format_large_number(int(total_usd_amount))}</code>\n"
            f"▸ Общий доход/10мин: <code>${format_large_number(int(total_income_amount))}</code>\n\n"
            
            "⚙️ <b>Система:</b>\n"
            f"▸ Время работы: <code>{days}д {hours}ч {minutes}м</code>\n"
            f"▸ Пинг: <code>{ping_ms}мс</code>\n"
            f"▸ Версия: <code>{version}</code>\n"
            f"▸ Последний рестарт: <code>{last_restart}</code>\n\n"
            
            "📊 <b>Дополнительно:</b>\n"
            f"▸ Франшиз: <code>{format_large_number(networks[0][0])}</code>\n"
            f"▸ Не боты: <code>{format_large_number(not_bots[0][0])}</code>"
        )
        
        await message.answer(response, parse_mode='HTML')
        
    except Exception as e:
        logger.error(f"Error in bot_info: {e}")
        await message.answer('❌ Ошибка при получении статистики')

@cmd_admin_router.message(Command('fix_income'))
async def cmd_fix_income(message: Message):
    """Пересчитывает доход всех пользователей на основе их компьютеров"""
    if message.from_user.id not in ADMIN:
        await message.answer('❌ У вас нет прав для использования этой команды')
        return

    try:
        # Получаем всех пользователей
        all_users = await execute_query('SELECT userid FROM stats')
        total_users = len(all_users)
        fixed_count = 0
        errors = 0

        status_msg = await message.answer(f"🔄 Начинаю пересчет дохода...\n👥 Всего пользователей: {total_users}")

        for i, (user_id,) in enumerate(all_users):
            try:
                # Получаем все компьютеры пользователя
                user_pcs = await execute_query('SELECT lvl, income FROM pc WHERE userid = ?', (user_id,))

                if not user_pcs:
                    # У пользователя нет компьютеров, устанавливаем доход в 0
                    await execute_update('UPDATE stats SET income = 0 WHERE userid = ?', (user_id,))
                    continue

                # Пересчитываем доход
                total_income = sum(Decimal(str(pc[1])) for pc in user_pcs)

                # Обновляем доход в базе
                await execute_update('UPDATE stats SET income = ? WHERE userid = ?', (float(total_income), user_id))
                fixed_count += 1

                # Обновляем статус каждые 50 пользователей
                if (i + 1) % 50 == 0:
                    progress = ((i + 1) / total_users) * 100
                    await status_msg.edit_text(
                        f"🔄 Пересчет дохода\n\n"
                        f"👥 Всего: {total_users}\n"
                        f"📊 Прогресс: {i + 1}/{total_users} ({progress:.1f}%)\n"
                        f"✅ Исправлено: {fixed_count}\n"
                        f"❌ Ошибок: {errors}"
                    )

            except Exception as e:
                logger.error(f"Error fixing income for user {user_id}: {e}")
                errors += 1
                continue

        # Финальное сообщение
        await status_msg.edit_text(
            f"✅ Пересчет дохода завершен!\n\n"
            f"👥 Всего пользователей: {total_users}\n"
            f"✅ Успешно обработано: {fixed_count}\n"
            f"❌ Ошибок: {errors}"
        )

    except Exception as e:
        logger.error(f"Error in fix_income command: {e}")
        await message.answer(f"❌ Произошла ошибка: {str(e)}")

@cmd_admin_router.message(Command('set_bal'))
async def cmd_set_bal(message: Message):
    if message.from_user.id in ADMIN:
        text_parts = message.text.split(' ')
        if len(text_parts) >= 2 and text_parts[1].isdigit():
            new_balance = int(text_parts[1])
            # Если указан третий параметр - это ID пользователя
            if len(text_parts) >= 3 and text_parts[2].isdigit():
                user_id = int(text_parts[2])
                await execute_update('UPDATE stats SET bal = ? WHERE userid = ?', (new_balance, user_id))
                await message.answer(f'✅ Баланс для пользователя {user_id} установлен: {new_balance}$')
            else:
                await execute_update('UPDATE stats SET bal = ? WHERE userid = ?', (new_balance, message.from_user.id))
                await message.answer(f'✅ Баланс установлен: {new_balance}$')
        else:
            await message.answer('⚠️ Используйте: /set_bal (сумма) [ID пользователя]')
    else:
        await message.answer('❌ У вас нет прав для использования этой команды')

@cmd_admin_router.message(Command('set'))
async def cmd_set(message: Message):
    if message.from_user.id in ADMIN:
        text_parts = message.text.split(' ')
        if len(text_parts) >= 4 and text_parts[2].isdigit() and text_parts[3].isdigit():
            column = text_parts[1]
            value = int(text_parts[2])
            user_id = int(text_parts[3])

            try:
                await execute_update(f'UPDATE stats SET {column} = ? WHERE userid = ?', (value, user_id))
                await message.answer('✅ Успешно обновлено')
            except Exception as e:
                await message.answer(f'❌ Ошибка: {str(e)}')
        else:
            await message.answer('⚠️ Используйте: /set (колонка) (значение) (ID пользователя)')

@cmd_admin_router.message(Command('delete'))
async def cmd_delete(message: Message):
    if message.from_user.id in ADMIN and TOKEN == '7391256097:AAGVbvFUMW5ShfffjsPFFvFl9QONZ2kJbu8':
        await execute_update('DELETE FROM stats')
        await execute_update('DELETE FROM pc')
        await execute_update('DELETE FROM networks')
        await execute_update('DELETE FROM orders')
        await execute_update('DELETE FROM promos')
        await execute_update('DELETE FROM titles')
        await execute_update('DELETE FROM messages')
        await execute_update('DELETE FROM chats')
        await execute_update('DELETE FROM ads')
        await execute_update('DELETE FROM actions')
        
        await message.answer('✅ База данных очищена')

@cmd_admin_router.message(Command('send_channel'))
async def cmd_send_channel(message: Message, state: FSMContext):
    if message.from_user.id in ADMIN:
        await bot.send_message(message.from_user.id, 'Укажите URL\nВведите /cancel для отмены действия')
        await state.set_state(Send_channel.url)

@cmd_admin_router.message(Command('test_weekly_reset'))
async def cmd_test_weekly_reset(message: Message):
    """Тестовая команда для проверки сброса недельных данных"""
    if message.from_user.id not in ADMIN:
        await message.answer('❌ Недостаточно прав')
        return

    try:
        await message.answer('🔄 Запуск тестового сброса...')

        # Получаем статистику ДО сброса
        stats = await calculate_weekly_stats()

        if stats:
            # Выдаем премиум топ-10
            winners = []
            used_positions = set()

            # Гарантированные победители: 8-е место
            if len(stats['top_franchises']) >= 8:
                franchise = stats['top_franchises'][7]
                days = random.randint(3, 7)
                success = await give_weekly_premium(franchise[2], days)
                if success:
                    winners.append({
                        'position': 8,
                        'franchise_name': franchise[0],
                        'franchise_id': franchise[2],
                        'days': days
                    })
                    used_positions.add(7)

            # Случайные 2 победителя из оставшихся позиций 4-10 (кроме 8-го)
            available_positions = [i for i in range(3, 10) if i != 7 and i < len(stats['top_franchises'])]

            if len(available_positions) >= 2:
                random_positions = random.sample(available_positions, 2)
                for pos in random_positions:
                    franchise = stats['top_franchises'][pos]
                    days = random.randint(2, 5)
                    success = await give_weekly_premium(franchise[2], days)
                    if success:
                        winners.append({
                            'position': pos + 1,
                            'franchise_name': franchise[0],
                            'franchise_id': franchise[2],
                            'days': days
                        })
                        used_positions.add(pos)

            # Отчет о выдаче премиума
            text = "✅ <b>ПРЕМИУМ ВЫДАН!</b>\n\n"
            for winner in winners:
                text += f"• {winner['position']} место: {winner['franchise_name']} (+{winner['days']} дней)\n"
            await message.answer(text, parse_mode='HTML')

            # Сбрасываем доход франшиз
            success = await reset_weekly_income()

            if success:
                await message.answer('✅ Доход франшиз и участников сброшен')
            else:
                await message.answer('❌ Ошибка при сбросе дохода')
        else:
            await message.answer('❌ Не удалось получить статистику')

    except Exception as e:
        logger.error(f"Error in test_weekly_reset: {e}")
        await message.answer(f'❌ Ошибка: {str(e)}')

@cmd_admin_router.message(Command('test_auto_promo'))
async def cmd_test_auto_promo(message: Message):
    """Тестовая команда для проверки автоматической генерации промокодов"""
    if message.from_user.id not in ADMIN:
        await message.answer('❌ Недостаточно прав')
        return

    try:
        await message.answer('🎁 Генерация промокода...')

        # Создаем промокод
        promo_code, promo_hours, promo_activations = await create_weekly_promo()

        if promo_code:
            text = (
                f"✅ <b>ПРОМОКОД СОЗДАН!</b>\n\n"
                f"🔑 Код: <code>{promo_code}</code>\n"
                f"💰 Награда: {promo_hours} часов дохода\n"
                f"👥 Активаций: {promo_activations}\n\n"
                f"Команда для активации: /promo {promo_code}"
            )
            await message.answer(text, parse_mode='HTML')
        else:
            await message.answer('❌ Ошибка при создании промокода')

    except Exception as e:
        logger.error(f"Error in test_auto_promo: {e}")
        await message.answer(f'❌ Ошибка: {str(e)}')

# ===== NETWORK CALLBACK HANDLERS =====
@cb_network_router.callback_query(F.data.startswith('network_members'))
async def cb_network_members(callback: CallbackQuery):
    userid = callback.data.split('_')[-1]
    user = await execute_query_one('SELECT userid, network FROM stats WHERE userid = ?', (callback.from_user.id,))
    
    if not user or user[0] != int(userid):
        await callback.answer('⚠️ Это не твое сообщение', show_alert=True)
        return
        
    await update_data(callback.from_user.username, callback.from_user.id)
    await add_action(callback.from_user.id, 'cb_network_members')
    
    user_data = user
    page = int(callback.data.split('_')[-2])
    
    # Получаем информацию о франшизе для проверки прав
    network_info = await execute_query('SELECT owner_id, admins FROM networks WHERE owner_id = ?', (user_data[1],))
    if not network_info:
        await callback.answer('❌ Франшиза не найдена', show_alert=True)
        return
        
    owner_id = network_info[0][0]
    admins = parse_array(network_info[0][1])
    
    is_owner = callback.from_user.id == owner_id
    is_admin = callback.from_user.id in admins
    
    # Получаем всех участников франшизы
    members = await execute_query('SELECT name, userid, net_inc FROM stats WHERE network = ? ORDER BY net_inc DESC', 
                           (user_data[1],))
    
    # Пагинация - по 5 участников на страницу
    members_per_page = 5
    total_members = len(members)
    total_pages = math.ceil(total_members / members_per_page)
    
    # Корректируем номер страницы
    if page < 1:
        page = 1
    elif page > total_pages:
        page = total_pages
    
    start_index = (page - 1) * members_per_page
    end_index = start_index + members_per_page
    current_members = members[start_index:end_index]
    
    # Формируем новый визуал
    text = f'👥 Участники франшизы:\n\n'
    
    for i, member in enumerate(current_members, start_index + 1):
        member_name = member[0] or "(Ник)"
        member_id = member[1]
        member_income = format_number_short(member[2], True)
        
        # Определяем статус
        if member_id == owner_id:
            status = "Владелец"
        elif member_id in admins:
            status = "Админ"
        else:
            status = "Участник"
        
        text += f'{i}. {member_name}\n'
        text += f'🆔: {member_id}\n'
        text += f'Доход: {member_income} 💸\n'
        text += f'Статус: {status}\n\n'
    
    # Добавляем информацию о странице
    text += f'📄 Страница {page}/{total_pages}'
    
    # Создаем клавиатуру с пагинацией
    keyboard_buttons = []
    
    # Кнопки пагинации
    pagination_buttons = []
    if page > 1:
        pagination_buttons.append(InlineKeyboardButton(text='⬅️ Назад', callback_data=f'network_members_{page-1}_{callback.from_user.id}'))
    
    if page < total_pages:
        pagination_buttons.append(InlineKeyboardButton(text='Вперед ➡️', callback_data=f'network_members_{page+1}_{callback.from_user.id}'))
    
    if pagination_buttons:
        keyboard_buttons.append(pagination_buttons)
    
    # Добавляем кнопку "Админ команды" только для владельца и админов
    if is_owner or is_admin:
        keyboard_buttons.append([InlineKeyboardButton(text='Админ команды', callback_data=f'admin_commands_{callback.from_user.id}')])
    
    keyboard_buttons.append([InlineKeyboardButton(text='🔙 Назад', callback_data=f'network_{callback.from_user.id}')])
    
    markup = InlineKeyboardMarkup(inline_keyboard=keyboard_buttons)
    
    await callback.message.edit_text(text, reply_markup=markup)
    
@cb_network_router.callback_query(F.data.startswith('admin_commands'))
async def cb_admin_commands(callback: CallbackQuery):
    userid = callback.data.split('_')[-1]
    user = await execute_query_one('SELECT userid, network FROM stats WHERE userid = ?', (callback.from_user.id,))
    
    if not user or user[0] != int(userid):
        await callback.answer('⚠️ Это не твое сообщение', show_alert=True)
        return
        
    # Проверяем права доступа
    user_data = user
    network_info = await execute_query('SELECT owner_id, admins FROM networks WHERE owner_id = ?', (user_data[1],))
    if not network_info:
        await callback.answer('❌ Франшиза не найдена', show_alert=True)
        return
        
    owner_id = network_info[0][0]
    admins = parse_array(network_info[0][1])
    
    if callback.from_user.id != owner_id and callback.from_user.id not in admins:
        await callback.answer('❌ Недостаточно прав', show_alert=True)
        return
    
    text = (
        'ℹ️ Админ команды:\n\n'
        'Исключить игрока - /delete_user\n\n'
        'Забанить игрока /ban_user\n\n'
        'Разбанить игрока /reban_user\n\n'
        'Выдать админку /set_admin\n\n'
        'Снять админку /delete_admin\n\n'
        '‼️ Правильное написание команды :\n'
        '/(команда) (id игрока)'
    )
    
    markup = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text='🔙 Назад к участникам', callback_data=f'network_members_1_{callback.from_user.id}')]
    ])
    
    await callback.message.edit_text(text, reply_markup=markup)    
    
@cb_network_router.callback_query(F.data.startswith('network_requests'))
async def cb_network_requests(callback: CallbackQuery):
    userid = callback.data.split('_')[-1]
    user = await execute_query_one('SELECT userid, network FROM stats WHERE userid = ?', (callback.from_user.id,))
    
    if not user or user[0] != int(userid):
        await callback.answer('⚠️ Это не твое сообщение', show_alert=True)
        return
        
    await update_data(callback.from_user.username, callback.from_user.id)
    await add_action(callback.from_user.id, 'cb_network_requests')
    
    user_data = user
    requests_result = await execute_query('SELECT requests FROM networks WHERE owner_id = ?', (user_data[1],))
    requests = parse_array(requests_result[0][0]) if requests_result else []
    
    text = '📫 Все заявки на вход:'
    num = 1
    for user_id in requests:
        user_data = await execute_query('SELECT name FROM stats WHERE userid = ?', (user_id,))
        user_name = user_data[0][0] if user_data else f"Пользователь {user_id}"
        text += f'\n{num}. {user_name}'
        text += f'\n🆔: <code>{user_id}</code>'  # Используем <code> для лучшего отображения ID
        num += 1
    
    text += '\n\n✅ Принять: /allow_user (id игрока*)\n❌ Отклонить: /reject_user (id игрока*)'
    text += '\n\n💡 ID можно скопировать, нажав на него'
    
    await callback.message.edit_text(text, parse_mode='HTML')

@cb_network_router.callback_query(F.data.startswith('network_edit_name'))
async def cb_network_edit_name(callback: CallbackQuery, state: FSMContext):
    userid = callback.data.split('_')[-1]
    user = await execute_query_one('SELECT userid FROM stats WHERE userid = ?', (callback.from_user.id,))
    
    if not user or user[0] != int(userid):
        await callback.answer('⚠️ Это не твое сообщение', show_alert=True)
        return
        
    await update_data(callback.from_user.username, callback.from_user.id)
    await add_action(callback.from_user.id, 'cb_network_edit_name')
    
    await callback.message.edit_text('📝 Введите новое название для франшизы\nВведите /cancel для отмены действия')
    await state.set_state(Network_edit.name)

@cb_network_router.callback_query(F.data.startswith('network_edit_description'))
async def cb_network_edit_description(callback: CallbackQuery, state: FSMContext):
    userid = callback.data.split('_')[-1]
    user = await execute_query_one('SELECT userid FROM stats WHERE userid = ?', (callback.from_user.id,))
    
    if not user or user[0] != int(userid):
        await callback.answer('⚠️ Это не твое сообщение', show_alert=True)
        return
        
    await update_data(callback.from_user.username, callback.from_user.id)
    await add_action(callback.from_user.id, 'cb_network_edit_description')
    
    await callback.message.edit_text('📝 Введите новое описание для франшизы\nВведите /cancel для отмены действия')
    await state.set_state(Network_edit.desc)

@cb_network_router.callback_query(F.data.startswith('network_type'))
async def cb_network_type(callback: CallbackQuery):
    userid = callback.data.split('_')[-1]
    user = await execute_query_one('SELECT userid, network FROM stats WHERE userid = ?', (callback.from_user.id,))
    
    if not user or user[0] != int(userid):
        await callback.answer('⚠️ Это не твое сообщение', show_alert=True)
        return
        
    await update_data(callback.from_user.username, callback.from_user.id)
    await add_action(callback.from_user.id, 'cb_network_type')
    
    user_data = user
    fran_type_result = await execute_query('SELECT type FROM networks WHERE owner_id = ?', (user_data[1],))
    fran_type = fran_type_result[0][0] if fran_type_result else 'open'
    net_type = callback.data.split('_')[-2]
    
    net_type2 = ''
    if net_type == 'open':
        net_type2 = 'Открытая'
    elif net_type == 'close':
        net_type2 = 'Закрытая'
    elif net_type == 'request':
        net_type2 = 'По заявке'
    
    if fran_type != net_type:
        if net_type != 'request':
            await execute_update("UPDATE networks SET requests = '[]' WHERE owner_id = ?", (user_data[1],))
        
        await execute_update('UPDATE networks SET type = ? WHERE owner_id = ?', (net_type, user_data[1]))
        
        markup = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text='🔙 Назад', callback_data=f'network_{callback.from_user.id}')]
        ])
        await callback.message.edit_text(f'✅ Вы успешно изменили статус франшизы на "{net_type2}"', reply_markup=markup)
    else:
        await callback.message.edit_text(f'⚠️ Ваша франшиза и так находится в статусе {net_type2.lower()}')

@cb_network_router.callback_query(F.data.startswith('network_edit_type'))
async def cb_network_edit_type(callback: CallbackQuery):
    userid = callback.data.split('_')[-1]
    user = await execute_query_one('SELECT userid FROM stats WHERE userid = ?', (callback.from_user.id,))
    
    if not user or user[0] != int(userid):
        await callback.answer('⚠️ Это не твое сообщение', show_alert=True)
        return
        
    await update_data(callback.from_user.username, callback.from_user.id)
    await add_action(callback.from_user.id, 'cb_network_edit_type')
    
    markup = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text='🔓 Открытая', callback_data=f'network_type_open_{callback.from_user.id}')],
        [InlineKeyboardButton(text='🔒 Закрытая', callback_data=f'network_type_close_{callback.from_user.id}')],
        [InlineKeyboardButton(text='✉️ По заявке', callback_data=f'network_type_request_{callback.from_user.id}')]
    ])
    
    await callback.message.edit_text('❓ Какой статус франшизы вы хотите установить?', reply_markup=markup)

@cb_network_router.callback_query(F.data.startswith('network_mailing'))
async def cb_network_mailing(callback: CallbackQuery, state: FSMContext):
    userid = callback.data.split('_')[-1]
    user = await execute_query_one('SELECT userid, network FROM stats WHERE userid = ?', (callback.from_user.id,))
    
    if not user or user[0] != int(userid):
        await callback.answer('⚠️ Это не твое сообщение', show_alert=True)
        return
        
    await update_data(callback.from_user.username, callback.from_user.id)
    await add_action(callback.from_user.id, 'cb_network_mailing')
    
    user_data = user
    network_result = await execute_query('SELECT admins, mailing FROM networks WHERE owner_id = ?', (user_data[1],))
    if not network_result:
        await callback.message.edit_text('❌ Франшиза не найдена')
        return
        
    network = network_result[0]
    admins = parse_array(network[0])
    
    mailing_date = datetime.datetime.strptime(network[1], '%Y-%m-%d %H:%M:%S') if isinstance(network[1], str) else network[1]
    
    if callback.from_user.id in admins or callback.from_user.id == user_data[1]:
        if mailing_date + datetime.timedelta(hours=1) <= datetime.datetime.now():
            await callback.message.edit_text('✉️ Введите текст для рассылки или /cancel для отмены действия')
            await state.set_state(Network_mailing.text)
        else:
            await callback.message.edit_text('⚠️ Рассылку можно отправлять только раз в час')
    else:
        await callback.message.edit_text('❌ Вы не являетесь владельцем или админом франшизы')

@cb_network_router.callback_query(F.data.startswith('network_edit'))
async def cb_network_edit(callback: CallbackQuery):
    userid = callback.data.split('_')[-1]
    user = await execute_query_one('SELECT userid FROM stats WHERE userid = ?', (callback.from_user.id,))
    
    if not user or user[0] != int(userid):
        await callback.answer('⚠️ Это не твое сообщение', show_alert=True)
        return
        
    await update_data(callback.from_user.username, callback.from_user.id)
    await add_action(callback.from_user.id, 'cb_network_edit')
    
    markup = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text='🪧 Название', callback_data=f'network_edit_name_{callback.from_user.id}')],
        [InlineKeyboardButton(text='💬 Описание', callback_data=f'network_edit_description_{callback.from_user.id}')],
        [InlineKeyboardButton(text='🔘 Статус', callback_data=f'network_edit_type_{callback.from_user.id}')],
        [InlineKeyboardButton(text='🔙 Назад', callback_data=f'network_{callback.from_user.id}')]
    ])
    
    await callback.message.edit_text('❓ Что будем изменять?', reply_markup=markup)

@cb_network_router.callback_query(F.data.startswith('network_delete_success'))
async def cb_network_delete_success(callback: CallbackQuery):
    userid = callback.data.split('_')[-1]
    user = await execute_query_one('SELECT userid FROM stats WHERE userid = ?', (callback.from_user.id,))
    
    if not user or user[0] != int(userid):
        await callback.answer('⚠️ Это не твое сообщение', show_alert=True)
        return
        
    await update_data(callback.from_user.username, callback.from_user.id)
    await add_action(callback.from_user.id, 'cb_network_delete_success')
    
    await execute_update('DELETE FROM networks WHERE owner_id = ?', (callback.from_user.id,))
    users = await execute_query('SELECT userid FROM stats WHERE network = ?', (callback.from_user.id,))
    
    for user_data in users:
        await execute_update('UPDATE stats SET network = NULL, net_inc = 0 WHERE userid = ?', (user_data[0],))
    
    await callback.message.edit_text('✅ Франшиза удалена!')

@cb_network_router.callback_query(F.data.startswith('network_delete'))
async def cb_network_delete(callback: CallbackQuery):
    userid = callback.data.split('_')[-1]
    user = await execute_query_one('SELECT userid FROM stats WHERE userid = ?', (callback.from_user.id,))
    
    if not user or user[0] != int(userid):
        await callback.answer('⚠️ Это не твое сообщение', show_alert=True)
        return
        
    await update_data(callback.from_user.username, callback.from_user.id)
    await add_action(callback.from_user.id, 'cb_network_delete')
    
    markup = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text='✅ Да.', callback_data=f'network_delete_success_{callback.from_user.id}')],
        [InlineKeyboardButton(text='❌ НЕТ!', callback_data=f'cancel_{callback.from_user.id}')]
    ])
    
    await callback.message.edit_text('‼️ Подтвердите удаление', reply_markup=markup)

@cb_network_router.callback_query(F.data.startswith('network_left_success'))
async def cb_network_left_success(callback: CallbackQuery):
    userid = callback.data.split('_')[-1]
    user = await execute_query_one('SELECT userid, net_inc, network FROM stats WHERE userid = ?', (callback.from_user.id,))
    
    if not user or user[0] != int(userid):
        await callback.answer('⚠️ Это не твое сообщение', show_alert=True)
        return
        
    await update_data(callback.from_user.username, callback.from_user.id)
    await add_action(callback.from_user.id, 'cb_network_left_success')
    
    user_data = user
    await execute_update('UPDATE stats SET network = NULL, net_inc = 0 WHERE userid = ?', (callback.from_user.id,))
    
    admins_result = await execute_query('SELECT admins FROM networks WHERE owner_id = ?', (user_data[2],))
    if admins_result and callback.from_user.id in parse_array(admins_result[0][0]):
        admins = parse_array(admins_result[0][0])
        new_admins = [admin for admin in admins if admin != callback.from_user.id]
        await execute_update('UPDATE networks SET admins = ? WHERE owner_id = ?', (format_array(new_admins), user_data[2]))
    
    await callback.message.edit_text('↩️ Вы покинули франшизу!')

@cb_network_router.callback_query(F.data.startswith('network_left'))
async def cb_network_left(callback: CallbackQuery):
    userid = callback.data.split('_')[-1]
    user = await execute_query_one('SELECT userid FROM stats WHERE userid = ?', (callback.from_user.id,))
    
    if not user or user[0] != int(userid):
        await callback.answer('⚠️ Это не твое сообщение', show_alert=True)
        return
        
    await update_data(callback.from_user.username, callback.from_user.id)
    await add_action(callback.from_user.id, 'cb_network_left')
    
    markup = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text='✅ Да.', callback_data=f'network_left_success_{callback.from_user.id}')],
        [InlineKeyboardButton(text='❌ НЕТ!', callback_data=f'cancel_{callback.from_user.id}')]
    ])
    
    await callback.message.edit_text('‼️ Подтвердите выход', reply_markup=markup)


# ===== ADMIN HANDLERS =====
@cmd_admin_router.message(Command('franchises'))
async def cmd_franchises(message: Message):
    """Показать все франшизы с их ID"""
    if message.from_user.id not in ADMIN:
        await message.answer('❌ Недостаточно прав')
        return
        
    await update_data(message.from_user.username, message.from_user.id)
    await add_action(message.from_user.id, 'cmd_franchises')
    
    # Получаем все франшизы
    franchises = await execute_query(
        'SELECT owner_id, name, income, description FROM networks ORDER BY income DESC LIMIT 50', 
    )
    
    if not franchises:
        await message.answer('❌ Франшизы не найдены')
        return
    
    text = '🏆 <b>Все франшизы:</b>\n\n'
    
    for i, franchise in enumerate(franchises, 1):
        franchise_id = franchise[0]
        franchise_name = franchise[1] if franchise[1] else "Без названия"
        franchise_income = franchise[2]
        franchise_desc = franchise[3] if franchise[3] else "Без описания"
        
        text += (
            f'{i}. <b>{franchise_name}</b>\n'
            f'🆔 ID: <code>{franchise_id}</code>\n'
            f'💰 Доход: {format_number_short(franchise_income, True)}$\n'
            f'📝 Описание: {franchise_desc[:50]}{"..." if len(franchise_desc) > 50 else ""}\n'
            f'❌ Удалить и заблокировать: /banfranchise {franchise_id}\n\n'
        )
    
    await message.answer(text, parse_mode='HTML')

@cmd_admin_router.message(Command('banfranchise'))
async def cmd_ban_franchise(message: Message):
    """Удалить франшизу и заблокировать владельца"""
    if message.from_user.id not in ADMIN:
        await message.answer('❌ Недостаточно прав')
        return
        
    text_parts = message.text.split(' ')
    
    if len(text_parts) != 2 or not text_parts[1].isdigit():
        await message.answer('⚠️ Используйте: /banfranchise (ID_франшизы)\n\n'
                           '📋 Список франшиз: /franchises')
        return
        
    franchise_id = int(text_parts[1])
    
    try:
        # Проверяем существование франшизы
        franchise = await execute_query_one(
            'SELECT name FROM networks WHERE owner_id = ?', 
            (franchise_id,)
        )
        
        if not franchise:
            await message.answer('❌ Франшиза не найдена')
            return
            
        franchise_name = franchise[0] if franchise[0] else "Без названия"
        
        # Получаем информацию о владельце
        owner = await execute_query_one(
            'SELECT name FROM stats WHERE userid = ?', 
            (franchise_id,)
        )
        owner_name = owner[0] if owner else f"Пользователь {franchise_id}"
        
        # Получаем количество участников
        members_count = await execute_query_one(
            'SELECT COUNT(*) FROM stats WHERE network = ?', 
            (franchise_id,)
        )
        members = members_count[0] if members_count else 0
        
        # Удаляем франшизу
        await execute_update('DELETE FROM networks WHERE owner_id = ?', (franchise_id,))
        
        # Обнуляем сеть у всех участников
        await execute_update(
            'UPDATE stats SET network = NULL, net_inc = 0 WHERE network = ?', 
            (franchise_id,)
        )
        
        # Блокируем владельца от создания франшиз
        await execute_update('''
            CREATE TABLE IF NOT EXISTS banned_franchise_users (
                user_id INTEGER PRIMARY KEY,
                banned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                banned_by INTEGER,
                reason TEXT DEFAULT "Запрет на создание франшиз"
            )
        ''')
        
        # Добавляем бан
        await execute_update(
            'INSERT OR REPLACE INTO banned_franchise_users (user_id, banned_by) VALUES (?, ?)', 
            (franchise_id, message.from_user.id)
        )
        
        await message.answer(
            f'✅ <b>Франшиза удалена и владелец заблокирован!</b>\n\n'
            f'🏷 Франшиза: <b>{franchise_name}</b>\n'
            f'🆔 ID франшизы: <code>{franchise_id}</code>\n'
            f'👤 Владелец: <b>{owner_name}</b>\n'
            f'👥 Участников: <b>{members}</b>\n'
            f'⏰ Время: <code>{datetime.datetime.now().strftime("%d.%m.%Y %H:%M")}</code>\n\n'
            f'🔓 Разблокировать: /unbanfranchise {franchise_id}',
            parse_mode='HTML'
        )
        
        # Уведомляем владельца
        try:
            await bot.send_message(
                franchise_id,
                '🚫 <b>Ваша франшиза была удалена!</b>\n\n'
                'Администратор удалил вашу франшизу и ограничил возможность создавать новые франшизы.',
                parse_mode='HTML'
            )
        except Exception as e:
            logger.warning(f"Could not notify franchise owner {franchise_id}: {e}")
            
    except Exception as e:
        logger.error(f"Error in ban_franchise: {e}")
        await message.answer('❌ Ошибка при удалении франшизы и блокировке владельца')

@cmd_admin_router.message(Command('unbanfranchise'))
async def cmd_unban_franchise(message: Message):
    """Разблокировать пользователя от создания франшиз"""
    if message.from_user.id not in ADMIN:
        await message.answer('❌ Недостаточно прав')
        return
        
    text_parts = message.text.split(' ')
    
    if len(text_parts) != 2 or not text_parts[1].isdigit():
        await message.answer('⚠️ Используйте: /unbanfranchise (ID_пользователя)')
        return
        
    user_id = int(text_parts[1])
    
    try:
        # Проверяем существование бана
        ban = await execute_query_one(
            'SELECT user_id FROM banned_franchise_users WHERE user_id = ?', 
            (user_id,)
        )
        
        if not ban:
            await message.answer('❌ Пользователь не заблокирован от создания франшиз')
            return
        
        # Удаляем бан
        await execute_update(
            'DELETE FROM banned_franchise_users WHERE user_id = ?', 
            (user_id,)
        )
        
        # Получаем имя пользователя
        user = await execute_query_one(
            'SELECT name FROM stats WHERE userid = ?', 
            (user_id,)
        )
        user_name = user[0] if user else f"Пользователь {user_id}"
        
        await message.answer(
            f'✅ <b>Пользователь разблокирован!</b>\n\n'
            f'👤 Пользователь: <b>{user_name}</b>\n'
            f'🆔 ID: <code>{user_id}</code>\n'
            f'⏰ Время: <code>{datetime.datetime.now().strftime("%d.%m.%Y %H:%M")}</code>',
            parse_mode='HTML'
        )
        
        # Уведомляем пользователя
        try:
            await bot.send_message(
                user_id,
                '✅ <b>Вам снова разрешено создавать франшизы!</b>\n\n'
                'Администратор снял ограничение на создание франшиз.',
                parse_mode='HTML'
            )
        except Exception as e:
            logger.warning(f"Could not notify user {user_id}: {e}")
            
    except Exception as e:
        logger.error(f"Error in unban_franchise: {e}")
        await message.answer('❌ Ошибка при разблокировке пользователя')

@cmd_admin_router.message(Command('bannedfranchise'))
async def cmd_banned_franchise(message: Message):
    """Показать список заблокированных пользователей"""
    if message.from_user.id not in ADMIN:
        await message.answer('❌ Недостаточно прав')
        return
        
    try:
        # Получаем список забаненных пользователей
        banned_users = await execute_query('''
            SELECT bf.user_id, bf.banned_at, bf.banned_by, bf.reason, s.name 
            FROM banned_franchise_users bf 
            LEFT JOIN stats s ON bf.user_id = s.userid 
            ORDER BY bf.banned_at DESC
        ''')
        
        if not banned_users:
            await message.answer('ℹ️ Нет пользователей с запретом на создание франшиз')
            return
        
        text = '🚫 <b>Заблокированные пользователи:</b>\n\n'
        
        for i, banned_user in enumerate(banned_users, 1):
            user_id = banned_user[0]
            banned_at = banned_user[1]
            banned_by = banned_user[2]
            reason = banned_user[3] or "Не указана"
            user_name = banned_user[4] or f"Пользователь {user_id}"
            
            # Форматируем дату
            if isinstance(banned_at, str):
                banned_date = banned_at[:16]
            else:
                banned_date = banned_at.strftime('%d.%m.%Y %H:%M') if hasattr(banned_at, 'strftime') else str(banned_at)[:16]
            
            text += (
                f'{i}. <b>{user_name}</b>\n'
                f'🆔 ID: <code>{user_id}</code>\n'
                f'⏰ Заблокирован: {banned_date}\n'
                f'📝 Причина: {reason}\n'
                f'🔓 Разблокировать: /unbanfranchise {user_id}\n\n'
            )
        
        await message.answer(text, parse_mode='HTML')
        
    except Exception as e:
        logger.error(f"Error in banned_franchise: {e}")
        await message.answer('❌ Ошибка при получении списка')


@cb_network_router.callback_query(F.data.startswith('network_create'))
async def cb_network_create(callback: CallbackQuery):
    userid = callback.data.split('_')[-1]
    user = await execute_query_one('SELECT userid, network FROM stats WHERE userid = ?', (callback.from_user.id,))
    
    if not user or user[0] != int(userid):
        await callback.answer('⚠️ Это не твое сообщение', show_alert=True)
        return
        
    await update_data(callback.from_user.username, callback.from_user.id)
    await add_action(callback.from_user.id, 'cb_network_create')
    
    user_data = user
    
    # ПРОВЕРКА НА БАН
    banned = await execute_query_one(
        'SELECT user_id FROM banned_franchise_users WHERE user_id = ?', 
        (callback.from_user.id,)
    )
    
    if banned:
        await callback.message.edit_text(
            '🚫 <b>Вам запрещено создавать франшизы!</b>\n\n'
            'Администратор ограничил вашу возможность создавать и управлять франшизами.',
            parse_mode='HTML'
        )
        return
    
    if user_data[1] is None:
        await execute_update('INSERT INTO networks (owner_id) VALUES (?)', (callback.from_user.id,))
        await execute_update('UPDATE stats SET network = ? WHERE userid = ?', (callback.from_user.id, callback.from_user.id))
        await callback.message.edit_text('✅ Вы успешно создали франшизу')
    else:
        await callback.message.edit_text('🫸 Вы уже состоите в франшизе')

@cb_network_router.callback_query(F.data.startswith('network_search_id'))
async def cb_network_search_id(callback: CallbackQuery, state: FSMContext):
    userid = callback.data.split('_')[-1]
    user = await execute_query_one('SELECT userid, network FROM stats WHERE userid = ?', (callback.from_user.id,))
    
    if not user or user[0] != int(userid):
        await callback.answer('⚠️ Это не твое сообщение', show_alert=True)
        return
        
    await update_data(callback.from_user.username, callback.from_user.id)
    await add_action(callback.from_user.id, 'cb_network_search_id')
    
    user_data = user
    if user_data[1] is None:
        await callback.message.edit_text('🆔 Введите ID или точное название франшизы в которую хотите вступить\nВведите /cancel для отмены действия')
        await state.set_state(Network_search.id)
    else:
        await callback.message.edit_text('🫸 Вы уже состоите в франшизе')

@cb_network_router.callback_query(F.data.startswith('network_search_num_'))
async def cb_network_search_num(callback: CallbackQuery):
    userid = callback.data.split('_')[-1]
    user = await execute_query_one('SELECT userid, network FROM stats WHERE userid = ?', (callback.from_user.id,))
    
    if not user or user[0] != int(userid):
        await callback.answer('⚠️ Это не твое сообщение', show_alert=True)
        return
        
    await update_data(callback.from_user.username, callback.from_user.id)
    await add_action(callback.from_user.id, 'cb_network_search_num')
    
    user_data = user
    if user_data[1] is None:
        franchises = await execute_query('SELECT owner_id, name, description, income FROM networks WHERE type != ? ORDER BY income DESC', 
                                  ('close',))
        
        if len(franchises) > 0:
            num = int(callback.data.split('_')[-2])
            franchise = franchises[num-1]
            
            text = f'Франшиза {franchise[1]}\n\n'
            text += f'Описание: {franchise[2]}\n'
            text += f'Заработано за эту неделю: {franchise[3]}'
            
            if len(franchises) == 1:
                markup = InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text=f'{num}', callback_data=f'{num}')],
                    [InlineKeyboardButton(text='Вступить', callback_data=f'network_join_{franchise[0]}_{callback.from_user.id}')]
                ])
            elif num == 1:
                markup = InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text=f'{num}', callback_data=f'{num}'),
                     InlineKeyboardButton(text=f'➡️', callback_data=f'network_search_num_{num+1}_{callback.from_user.id}')],
                    [InlineKeyboardButton(text='Вступить', callback_data=f'network_join_{franchise[0]}_{callback.from_user.id}')]
                ])
            elif num == len(franchises):
                markup = InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text=f'⬅️', callback_data=f'network_search_num_{num-1}_{callback.from_user.id}'),
                     InlineKeyboardButton(text=f'{num}', callback_data=f'{num}')],
                    [InlineKeyboardButton(text='Вступить', callback_data=f'network_join_{franchise[0]}_{callback.from_user.id}')]
                ])
            else:
                markup = InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text=f'⬅️', callback_data=f'network_search_num_{num-1}_{callback.from_user.id}'),
                     InlineKeyboardButton(text=f'{num}', callback_data=f'{num}'),
                     InlineKeyboardButton(text=f'➡️', callback_data=f'network_search_num_{num+1}_{callback.from_user.id}')],
                    [InlineKeyboardButton(text='Вступить', callback_data=f'network_join_{franchise[0]}_{callback.from_user.id}')]
                ])
            
            await callback.message.edit_text(text, reply_markup=markup)
        else:
            await callback.message.edit_text('⚠️ Франшиз пока нет, но вы можете создать первую')
    else:
        await callback.message.edit_text('🫸 Вы уже состоите в франшизе')

@cb_network_router.callback_query(F.data.startswith('network_owner'))
async def cb_network_owner(callback: CallbackQuery, state: FSMContext):
    userid = callback.data.split('_')[-1]
    user = await execute_query_one('SELECT userid FROM stats WHERE userid = ?', (callback.from_user.id,))
    
    if not user or user[0] != int(userid):
        await callback.answer('⚠️ Это не твое сообщение', show_alert=True)
        return
        
    await update_data(callback.from_user.username, callback.from_user.id)
    await add_action(callback.from_user.id, 'cb_network_owner')
    
    await callback.message.answer('🆔 Введите ID пользователя которого хотите назначить владельцем франшизы или /cancel для отмены действия')
    await state.set_state(Reowner.userid)

@cb_network_router.callback_query(F.data.startswith('network_search_'))
async def cb_network_search(callback: CallbackQuery):
    userid = callback.data.split('_')[-1]
    user = await execute_query_one('SELECT userid FROM stats WHERE userid = ?', (callback.from_user.id,))
    
    if not user or user[0] != int(userid):
        await callback.answer('⚠️ Это не твое сообщение', show_alert=True)
        return
        
    await update_data(callback.from_user.username, callback.from_user.id)
    await add_action(callback.from_user.id, 'cb_network_search')
    
    markup = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f'📜 Доступные франшизы', callback_data=f'network_search_num_1_{callback.from_user.id}')],
        [InlineKeyboardButton(text=f'🔍 Поиск по ID или названию', callback_data=f'network_search_id_{callback.from_user.id}')],
    ])
    
    await callback.message.edit_text('❓ Выберите метод поиска франшизы:', reply_markup=markup)

@cb_network_router.callback_query(F.data.startswith('network_join'))
async def cb_network_join(callback: CallbackQuery):
    userid = callback.data.split('_')[-1]
    user = await execute_query_one('SELECT userid, network FROM stats WHERE userid = ?', (callback.from_user.id,))
    
    if not user or user[0] != int(userid):
        await callback.answer('⚠️ Это не твое сообщение', show_alert=True)
        return
        
    await update_data(callback.from_user.username, callback.from_user.id)
    await add_action(callback.from_user.id, 'cb_network_join')
    
    user_data = user
    data = callback.data.split('_')
    network_id = int(data[2])
    
    if user_data[1] is None:
        info = await execute_query('SELECT type, requests, ban_users, admins FROM networks WHERE owner_id = ?', 
                            (network_id,))
        
        if info:
            info = info[0]
            network_type = info[0]
            requests = parse_array(info[1])
            ban_users = parse_array(info[2])
            admins = parse_array(info[3])
            
            if callback.from_user.id not in ban_users:
                if network_type == 'open':
                    await execute_update('UPDATE stats SET network = ? WHERE userid = ?', (network_id, callback.from_user.id))
                    await callback.message.edit_text('🤝 Вы успешно присоединились к франшизе!')
                elif network_type == 'close':
                    await callback.message.edit_text('🔒 Эта франшиза является закрытой!')
                elif network_type == 'request':
                    new_requests = requests
                    new_requests.append(callback.from_user.id)
                    await execute_update('UPDATE networks SET requests = ? WHERE owner_id = ?', 
                                 (format_array(new_requests), network_id))
                    await callback.message.edit_text('📨 Вы успешно подали заявку на вступление!')
                    
                    for admin in admins:
                        markup = InlineKeyboardMarkup(inline_keyboard=[
                            [InlineKeyboardButton(text='📫 Заявки', callback_data=f'network_requests_{admin}')]
                        ])
                        await bot.send_message(admin, '📬 Вам пришла заявка на вступление в франшизу', reply_markup=markup)
                    
                    markup = InlineKeyboardMarkup(inline_keyboard=[
                        [InlineKeyboardButton(text='📫 Заявки', callback_data=f'network_requests_{network_id}')]
                    ])
                    await bot.send_message(network_id, '📬 Вам пришла заявка на вступление в франшизу', reply_markup=markup)
            else:
                await callback.message.edit_text('😔 Вы были добавлены в черный список этой франшизы, и по этому не можете в нее вступить')
        else:
            await callback.message.edit_text('❌ Франшиза не найдена')
    else:
        await callback.message.edit_text('🫸 Вы уже состоите в франшизе')

@cb_network_router.callback_query(F.data.startswith('network'))
async def cb_network(callback: CallbackQuery):
    userid = callback.data.split('_')[-1]
    user = await execute_query_one('SELECT userid, network FROM stats WHERE userid = ?', (callback.from_user.id,))
    
    if not user or user[0] != int(userid):
        await callback.answer('⚠️ Это не твое сообщение', show_alert=True)
        return
        
    await update_data(callback.from_user.username, callback.from_user.id)
    await add_action(callback.from_user.id, 'cb_network')
    
    user_data = user
    if user_data[1] is None:
        markup = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text='🆕 Создать новую франшизу', callback_data=f'network_create_{callback.from_user.id}')],
            [InlineKeyboardButton(text='🤝 Вступить в франшизу', callback_data=f'network_search_{callback.from_user.id}')]
        ])
        await callback.message.edit_text('🌐 Вы не состоите в франшизе', reply_markup=markup)
    else:
        network = await execute_query('SELECT name, owner_id, description, income, type, admins FROM networks WHERE owner_id = ?', 
                               (user_data[1],))
        
        if network:
            network = network[0]
            if network[4] == 'request':
                markup1 = InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text='✏️ Изменить франшизу', callback_data=f'network_edit_{callback.from_user.id}')],
                    [InlineKeyboardButton(text='👥 Участники', callback_data=f'network_members_1_{callback.from_user.id}')],
                    [InlineKeyboardButton(text='📫 Заявки', callback_data=f'network_requests_{callback.from_user.id}')],
                    [InlineKeyboardButton(text='📤 Сделать рассылку', callback_data=f'network_mailing_{callback.from_user.id}')]
                ])
            else:
                markup1 = InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text='✏️ Изменить франшизу', callback_data=f'network_edit_{callback.from_user.id}')],
                    [InlineKeyboardButton(text='👥 Участники', callback_data=f'network_members_1_{callback.from_user.id}')],
                    [InlineKeyboardButton(text='📤 Сделать рассылку', callback_data=f'network_mailing_{callback.from_user.id}')]
                ])
            
            if network[1] == callback.from_user.id:
                markup1.inline_keyboard.extend([
                    [InlineKeyboardButton(text='🔄️ Передать права на франшизу', callback_data=f'network_owner_{callback.from_user.id}')],
                    [InlineKeyboardButton(text='🗑️ Удалить франшизу', callback_data=f'network_delete_{callback.from_user.id}')]
                ])
            else:
                markup1.inline_keyboard.append([InlineKeyboardButton(text='↩️ Покинуть франшизу', callback_data=f'network_left_{callback.from_user.id}')])
            
            net_type = ''
            if network[4] == 'open':
                net_type = 'Открытая'
            elif network[4] == 'close':
                net_type = 'Закрытая'
            elif network[4] == 'request':
                net_type = 'По заявке'
            
            members = await execute_query('SELECT COUNT(*) FROM stats WHERE network = ?', (network[1],))
            admins = parse_array(network[5])
            
            if network[1] == callback.from_user.id or callback.from_user.id in admins:
                await callback.message.edit_text(
                    f'🌐 Франшиза {network[0]}\n\n'
                    f'🆔 ID: {network[1]}\n'
                    f'💭 Описание: {network[2]}\n'
                    f'🔘 Статус: {net_type}\n\n'
                    f'👥 Количество клубов-участников: {members[0][0]}\n\n'
                    f'💰 Заработано за эту неделю: {network[3]}$\n'
                    f'🏆 Топ франшизы: /franchise_info', 
                    reply_markup=markup1
                )
            else:
                markup2 = InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text='↩️ Покинуть франшизу', callback_data=f'network_left_{callback.from_user.id}')]
                ])
                await callback.message.edit_text(
                    f'🌐 Франшиза {network[0]}\n\n'
                    f'🆔 ID: {network[1]}\n'
                    f'💭 Описание: {network[2]}\n'
                    f'🔘 Статус: {net_type}\n\n'
                    f'👥 Количество клубов-участников: {members[0][0]}\n\n'
                    f'💰 Заработано за эту неделю: {network[3]}$\n'
                    f'🏆 Топ франшизы: /franchise_info', 
                    reply_markup=markup2
                )

# ===== GAMES CALLBACK HANDLERS =====
@cb_games_router.callback_query(F.data.startswith('game_1'))
async def cb_game_1(callback: CallbackQuery, state: FSMContext):
    userid = callback.data.split('_')[-1]
    user = await execute_query_one('SELECT userid FROM stats WHERE userid = ?', (callback.from_user.id,))
    
    if not user or user[0] != int(userid):
        await callback.answer('⚠️ Это не твое сообщение', show_alert=True)
        return
        
    await update_data(callback.from_user.username, callback.from_user.id)
    await add_action(callback.from_user.id, 'cb_game_1')
    
    await callback.message.edit_text('❓ На что вы хотите сделать ставку?\nВведите орел/решка или /cancel для отмены действия')
    await state.set_state(Games.game1_bet)

@cb_games_router.callback_query(F.data.startswith('game_2'))
async def cb_game_2(callback: CallbackQuery, state: FSMContext):
    userid = callback.data.split('_')[-1]
    user = await execute_query_one('SELECT userid FROM stats WHERE userid = ?', (callback.from_user.id,))
    
    if not user or user[0] != int(userid):
        await callback.answer('⚠️ Это не твое сообщение', show_alert=True)
        return
        
    await update_data(callback.from_user.username, callback.from_user.id)
    await add_action(callback.from_user.id, 'cb_game_2')
    
    await callback.message.edit_text('❓ На что вы хотите сделать ставку?\nВведите число от 1 до 6 или /cancel для отмены действия')
    await state.set_state(Games.game2_bet)

# ===== ECONOMY CALLBACK HANDLERS =====

@cb_economy_router.callback_query(F.data.startswith('shop_pc'))
async def cb_shop_pc(callback: CallbackQuery):
    userid = callback.data.split('_')[-1]
    user = await execute_query_one('SELECT userid, room FROM stats WHERE userid = ?', (callback.from_user.id,))
    
    if not user or user[0] != int(userid):
        await callback.answer('⚠️ Это не твое сообщение', show_alert=True)
        return
        
    await update_data(callback.from_user.username, callback.from_user.id)
    await add_action(callback.from_user.id, 'cb_shop_pc')
    
    user_data = user
    available_pcs = await get_available_pcs(callback.from_user.id)
    
    text = '🖥️ Доступные компьютеры:\n\n'
    
    # Показываем только последние 6 доступных ПК
    for pc in available_pcs[-6:]:
        text += f'Компьютер {pc[0]} ур. Доход: {format_number_short(pc[1], True)}$ / 10 мин.\nЦена: {format_number_short(pc[2], True)}$ Купить: /buy_{pc[0]}\n\n'
    
    text += f'🛒 Купить компьютер:\n/buy_(уровень компьютера*) (количество)'
    
    markup = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text='🔙 Назад', callback_data=f'shop_{callback.from_user.id}')]
    ])
    
    await callback.message.edit_text(text, reply_markup=markup)
@cb_economy_router.callback_query(F.data.startswith('shop_ads'))
async def cb_shop_ads(callback: CallbackQuery):
    userid = callback.data.split('_')[-1]
    user = await execute_query_one('SELECT userid FROM stats WHERE userid = ?', (callback.from_user.id,))
    
    if not user or user[0] != int(userid):
        await callback.answer('⚠️ Это не твое сообщение', show_alert=True)
        return
        
    await update_data(callback.from_user.username, callback.from_user.id)
    await add_action(callback.from_user.id, 'cb_shop_ads')
    
    text = '📢 Реклама\n\n'
    
    for ad in ads:
        emoji = ['1⃣', '2⃣', '3⃣', '4⃣', '5⃣'][ad[0]-1]
        text += f'{emoji} {ad[1]}\n'
        text += f'Цена: {ad[2]}$\n'
        text += f'Бонус: +{ad[3]}%\n'
        text += f'Срок: {ad[4]}ч.\n'
        text += f'Откат: {ad[5]}ч.\n\n'
    
    markup = InlineKeyboardMarkup(inline_keyboard=[])
    
    for i in range(0, len(ads), 3):
        row_ads = ads[i:i+3]
        row_buttons = []
        for ad in row_ads:
            row_buttons.append(InlineKeyboardButton(text=f'{ad[0]}) {ad[1][0]}', callback_data=f'buy_ad{ad[0]}_{callback.from_user.id}'))
        markup.inline_keyboard.append(row_buttons)
    
    markup.inline_keyboard.append([InlineKeyboardButton(text='🔙 Назад', callback_data=f'shop_{callback.from_user.id}')])
    
    await callback.message.edit_text(text, reply_markup=markup)

@cb_economy_router.callback_query(F.data.startswith('buy_ad'))
async def cb_buy_ad(callback: CallbackQuery):
    userid = callback.data.split('_')[-1]
    user = await execute_query_one('SELECT userid, bal FROM stats WHERE userid = ?', (callback.from_user.id,))
    
    if not user or user[0] != int(userid):
        await callback.answer('⚠️ Это не твое сообщение', show_alert=True)
        return
        
    await update_data(callback.from_user.username, callback.from_user.id)
    await add_action(callback.from_user.id, 'cb_buy_ad')
    
    user_data = user
    user_ad = await execute_query('SELECT * FROM ads WHERE userid = ? ORDER BY dt DESC LIMIT 1', 
                           (callback.from_user.id,))
    
    success = 0
    remaining_time = None
    
    if not user_ad:
        success = 1
    else:
        user_ad = user_ad[0]
        for ad in ads:
            if user_ad[2] == ad[0]:
                ad_dt = datetime.datetime.strptime(user_ad[4], '%Y-%m-%d %H:%M:%S') if isinstance(user_ad[4], str) else user_ad[4]
                cooldown_end = ad_dt + datetime.timedelta(hours=ad[4] + ad[5])
                now = datetime.datetime.now()
                
                if cooldown_end < now:
                    success = 1
                else:
                    # Вычисляем оставшееся время до конца кулдауна
                    time_left = cooldown_end - now
                    hours_left = int(time_left.total_seconds() // 3600)
                    minutes_left = int((time_left.total_seconds() % 3600) // 60)
                    remaining_time = f"{hours_left}ч {minutes_left}м"
                break
    
    if success != 1:
        if remaining_time:
            await callback.message.edit_text(f'⚠️ Вы недавно уже покупали рекламу\n⏳ Доступно через: {remaining_time}')
        else:
            await callback.message.edit_text('⚠️ Вы недавно уже покупали рекламу')
        return
    
    ad_num = int(callback.data[6])
    for ad in ads:
        if ad[0] == ad_num:
            if user_data[1] >= ad[2]:
                await execute_update('UPDATE stats SET bal = bal - ? WHERE userid = ?', (ad[2], callback.from_user.id))
                await execute_update('INSERT INTO ads (userid, num, percent, dt) VALUES (?, ?, ?, ?)',
                             (callback.from_user.id, ad[0], ad[3], datetime.datetime.now()))
                await callback.message.edit_text(f'✅ Вы успешно купили рекламу {ad[1]}')
            else:
                await callback.message.edit_text(f'❌ Недостаточно средств')
            break
        
@cb_economy_router.callback_query(F.data.startswith('shop_room'))
async def cb_shop_room(callback: CallbackQuery):
    userid = callback.data.split('_')[-1]
    user = await execute_query_one('SELECT userid, room FROM stats WHERE userid = ?', (callback.from_user.id,))
    
    if not user or user[0] != int(userid):
        await callback.answer('⚠️ Это не твое сообщение', show_alert=True)
        return
        
    await update_data(callback.from_user.username, callback.from_user.id)
    await add_action(callback.from_user.id, 'cb_shop_room')
    
    user_data = user
    available_upgrades = await get_room_upgrades(callback.from_user.id)
    
    current_room_name = ROOM_NAMES.get(user_data[1], f"Комната {user_data[1]}")
    
    if available_upgrades:
        next_upgrade = available_upgrades[0]
        next_room_name = ROOM_NAMES.get(next_upgrade[0], f"Комната {next_upgrade[0]}")
        
        markup = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text='⏫ Улучшить', callback_data=f'update_room_{callback.from_user.id}')],
            [InlineKeyboardButton(text='🔙 Назад', callback_data=f'shop_{callback.from_user.id}')]
        ])
        
        await callback.message.edit_text(
            f'🏢 Комната: {current_room_name}\n'
            f'🆙 Уровень: {user_data[1]}\n\n'
            f'Следующий уровень: {next_room_name}\n\n'
            f'Минимальный доход для улучшения: {format_number_short(next_upgrade[2], True)}$\n'
            f'Цена улучшения: {format_number_short(next_upgrade[1], True)}$', 
            reply_markup=markup
        )
    else:
        markup = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text='🔙 Назад', callback_data=f'shop_{callback.from_user.id}')]
        ])
        
        await callback.message.edit_text(
            f'🏢 Комната: {current_room_name}\n'
            f'🆙 Уровень: {user_data[1]}\n\n'
            f'❇️ Максимальный уровень!', 
            reply_markup=markup
        )


@cb_economy_router.callback_query(F.data.startswith('shop_upgrade'))
async def cb_shop_upgrade(callback: CallbackQuery):
    userid = callback.data.split('_')[-1]
    user = await execute_query_one('SELECT userid, upgrade_internet, upgrade_devices, upgrade_service FROM stats WHERE userid = ?', 
                        (callback.from_user.id,))
    
    if not user or user[0] != int(userid):
        await callback.answer('⚠️ Это не твое сообщение', show_alert=True)
        return
        
    await update_data(callback.from_user.username, callback.from_user.id)
    await add_action(callback.from_user.id, 'cb_shop_upgrade')
    
    user_data = user
    text = '🔧 Улучшения отеля:'
    els = [
        [1, '📶 Интернет', 'upgrade_internet', user_data[1]],
        [2, '💻 Девайсы', 'upgrade_devices', user_data[2]],
        [3, '⭐ Сервис', 'upgrade_service', user_data[3]]
    ]
    
    total_bonus = 0
    
    for el in els:
        current_level = el[3]
        total_bonus += current_level
        
        # Проверяем, достигнут ли максимум
        if current_level == 5:
            text += f'\n\n{el[1]}: {current_level}/5 (+{current_level}%) - максимум'
        else:
            # Ищем стоимость следующего улучшения
            for upg in upgrade:
                if current_level + 1 == upg[0]:
                    text += f'\n\n{el[1]}: {current_level}/5 (+{current_level}%)\nСледующий уровень: {upg[1]}$ - /{el[2]}'
                    break
    
    text += f'\n\n📊 Общий бонус от улучшений: +{total_bonus}% к доходу'
    
    markup = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text='🔙 Назад', callback_data=f'shop_{callback.from_user.id}')]
    ])
    
    await callback.message.edit_text(text, reply_markup=markup)
    
@cb_economy_router.callback_query(F.data.startswith('update_room'))
async def cb_update_room(callback: CallbackQuery):
    userid = callback.data.split('_')[-1]
    user = await execute_query_one('SELECT userid, room, bal, income FROM stats WHERE userid = ?', (callback.from_user.id,))
    
    if not user or user[0] != int(userid):
        await callback.answer('⚠️ Это не твое сообщение', show_alert=True)
        return
        
    await update_data(callback.from_user.username, callback.from_user.id)
    await add_action(callback.from_user.id, 'cb_update_room')
    
    user_data = user
    available_upgrades = await get_room_upgrades(callback.from_user.id)
    
    if not available_upgrades:
        await callback.message.edit_text('❌ Достигнут максимальный уровень комнаты!')
        return
    
    next_upgrade = available_upgrades[0]
    
    if user_data[2] >= next_upgrade[1] and user_data[3] >= next_upgrade[2]:
        # Рассчитываем репутацию за улучшение комнаты
        rep_points = 20 + (user_data[1] * 10)  # 20 за 2 уровень, +10 за каждый следующий
        new_points, new_level, level_up = await add_reputation(
            callback.from_user.id, rep_points, "upgrade_room"
        )
        
        await execute_update('UPDATE stats SET bal = bal - ?, room = room + 1 WHERE userid = ?', 
                     (next_upgrade[1], callback.from_user.id))
        
        # Получаем обновленный баланс
        updated_user = await execute_query_one('SELECT bal FROM stats WHERE userid = ?', (callback.from_user.id,))
        new_balance = updated_user[0] if updated_user else user_data[2] - next_upgrade[1]
        
        # Новый визуал сообщения с репутацией
        room_name = ROOM_NAMES.get(user_data[1] + 1, f"Комната {user_data[1] + 1}")
        success_text = (
            f'✅ Вы успешно прокачали комнату\n'
            f'🏢 Теперь у вас: {room_name}\n'
            f'✨ +{rep_points} Репутации\n'
            f'💰Ваш баланс - {format_number_short(new_balance, True)}$'
        )
        
        # Если повысился уровень репутации
        if level_up:
            rep_info = await get_current_reputation_info(callback.from_user.id)
            success_text += f"\n\n🎉 Новый уровень репутации: {rep_info['level_name']}!"
        
        # Создаем клавиатуру с кнопкой "Назад"
        markup = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(
                text="Назад", 
                callback_data=f'shop_room_{callback.from_user.id}'
            )]
        ])
        
        await callback.message.edit_text(success_text, reply_markup=markup)
        
        # Бонус за первое улучшение комнаты (только для уровня 2)
        if user_data[1] + 1 == 2:
            ref = await execute_query('SELECT ref FROM stats WHERE userid = ?', (callback.from_user.id,))
            if ref and ref[0][0]:
                prem = await execute_query('SELECT premium FROM stats WHERE userid = ?', (ref[0][0],))
                if prem:
                    premium_date = datetime.datetime.strptime(prem[0][0], '%Y-%m-%d %H:%M:%S') if isinstance(prem[0][0], str) else prem[0][0]
                    if premium_date > datetime.datetime.now():
                        new_premium = premium_date + datetime.timedelta(hours=12)
                    else:
                        new_premium = datetime.datetime.now() + datetime.timedelta(hours=12)
                    await execute_update('UPDATE stats SET premium = ? WHERE userid = ?', (new_premium, ref[0][0]))
    elif user_data[2] < next_upgrade[1]:
        await callback.message.edit_text('❌ У вас не хватает $')
    elif user_data[3] < next_upgrade[2]:
        await callback.message.edit_text(f'❌ У вас недостаточно дохода, нужно: {format_number_short(next_upgrade[2], True)}$')

        
@cb_economy_router.callback_query(F.data.startswith('bonus'))
async def cb_bonus(callback: CallbackQuery):
    userid = callback.data.split('_')[-1]
    user = await execute_query_one('SELECT userid, bonus, bal, income, all_wallet FROM stats WHERE userid = ?', 
                        (callback.from_user.id,))
    
    if not user or user[0] != int(userid):
        await callback.answer('⚠️ Это не твое сообщение', show_alert=True)
        return
        
    await update_data(callback.from_user.username, callback.from_user.id)
    await add_action(callback.from_user.id, 'cb_bonus')
    
    user_data = user
    user_income = Decimal(str(user_data[3]))
    
    if user_data[1] == 1:
        x = 5
        percent = random.randint(1, 100)
        if percent <= 5:
            x = 20
        elif percent <= 15:
            x = 15
        elif percent <= 30:
            x = 10
        elif percent <= 50:
            x = 6
        
        total = user_income * x * 6
        
        # УБИРАЕМ зачисление на франшизу - бонус идет только на личный баланс
        await execute_update('UPDATE stats SET bonus = 0, bal = bal + ?, all_wallet = all_wallet + ? WHERE userid = ?', 
                     (float(total), float(total), callback.from_user.id))
        
        await callback.message.edit_text(f'✨ Вы успешно получили {format_number_short(total, True)}$')
    else:
        await callback.message.edit_text('🕛 Ежедневный бонус ещё не доступен, он обновляется каждый день в 00:00 по МСК')

@cb_economy_router.callback_query(F.data.startswith('shop'))
async def cb_shop(callback: CallbackQuery):
    userid = callback.data.split('_')[-1]
    user = await execute_query_one('SELECT userid FROM stats WHERE userid = ?', (callback.from_user.id,))
    
    if not user or user[0] != int(userid):
        await callback.answer('⚠️ Это не твое сообщение', show_alert=True)
        return
        
    await update_data(callback.from_user.username, callback.from_user.id)
    await add_action(callback.from_user.id, 'cb_shop')
    
    markup = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text='🖥 Компьютеры', callback_data=f'shop_pc_{callback.from_user.id}')],
        [InlineKeyboardButton(text='⏫ Комната', callback_data=f'shop_room_{callback.from_user.id}')],
        [InlineKeyboardButton(text='🔧 Улучшения', callback_data=f'shop_upgrade_{callback.from_user.id}')],
        [InlineKeyboardButton(text='📢 Реклама', callback_data=f'shop_ads_{callback.from_user.id}')]
    ])
    
    await callback.message.edit_text('🛒 PC Club Shop\nВыберите раздел:', reply_markup=markup)

# ===== DONATE CALLBACK HANDLERS =====
@cb_donate_router.callback_query(F.data.startswith('donate_premium'))
async def cb_donate_premium(callback: CallbackQuery):
    userid = callback.data.split('_')[-1]
    user = await execute_query_one('SELECT userid, premium FROM stats WHERE userid = ?', (callback.from_user.id,))

    if not user or user[0] != int(userid):
        await callback.answer('⚠️ Это не твое сообщение', show_alert=True)
        return

    await update_data(callback.from_user.username, callback.from_user.id)
    await add_action(callback.from_user.id, 'cb_donate_premium')

    # Проверка активного премиума
    premium_status = ''
    try:
        if user[1]:
            premium_date = datetime.datetime.strptime(user[1], '%Y-%m-%d %H:%M:%S') if isinstance(user[1], str) else user[1]
            if premium_date > datetime.datetime.now():
                remaining = premium_date - datetime.datetime.now()
                days = remaining.days
                if days > 30:
                    months = days // 30
                    premium_status = f'\n\n💎 Ваш PREMIUM активен ещё {months} месяц(ев)'
                elif days > 0:
                    premium_status = f'\n\n💎 Ваш PREMIUM активен ещё {days} день/дней'
                else:
                    hours = remaining.seconds // 3600
                    premium_status = f'\n\n💎 Ваш PREMIUM активен ещё {hours} час(ов)'
    except Exception as e:
        logger.error(f"Error parsing premium date: {e}")

    markup = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text='1 день - 40₽', callback_data=f'premium_1day_{callback.from_user.id}')],
        [InlineKeyboardButton(text='3 дня - 100₽', callback_data=f'premium_3days_{callback.from_user.id}')],
        [InlineKeyboardButton(text='1 неделя - 225₽', callback_data=f'premium_1week_{callback.from_user.id}')],
        [InlineKeyboardButton(text='1 месяц - 500₽', callback_data=f'premium_1month_{callback.from_user.id}')],
        [InlineKeyboardButton(text='◀️ Назад', callback_data=f'donate_back_{callback.from_user.id}')]
    ])

    await callback.message.edit_text(
        '👑 PREMIUM Статус\n\n'
        '💫 Бонусы:\n'
        '• 🎛 +35% к доходу фермы\n'
        '• 🎁 Ежедневный бонус раз в 12 часов\n'
        '• ⚡ Приоритетная поддержка\n\n'
        '📅 Выберите срок:' + premium_status,
        reply_markup=markup
    )

@cb_donate_router.callback_query(F.data.startswith('donate_sponsor'))
async def cb_donate_sponsor(callback: CallbackQuery):
    userid = callback.data.split('_')[-1]
    user = await execute_query_one('SELECT userid FROM stats WHERE userid = ?', (callback.from_user.id,))

    if not user or user[0] != int(userid):
        await callback.answer('⚠️ Это не твое сообщение', show_alert=True)
        return

    await update_data(callback.from_user.username, callback.from_user.id)
    await add_action(callback.from_user.id, 'cb_donate_sponsor')

    markup = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text='1 день - 35₽', callback_data=f'sponsor_1day_{callback.from_user.id}')],
        [InlineKeyboardButton(text='3 дня - 75₽', callback_data=f'sponsor_3days_{callback.from_user.id}')],
        [InlineKeyboardButton(text='1 неделя - 150₽', callback_data=f'sponsor_1week_{callback.from_user.id}')],
        [InlineKeyboardButton(text='1 месяц - 400₽', callback_data=f'sponsor_1month_{callback.from_user.id}')],
        [InlineKeyboardButton(text='◀️ Назад', callback_data=f'donate_back_{callback.from_user.id}')]
    ])

    await callback.message.edit_text(
        '👨‍💻 Спонсор клуба:\n\n'
        '✨ Бонус: +25% к доходу клуба\n\n'
        '📊 Особенности:\n'
        '• 🎯 Бонусы Спонсоров суммируются\n'
        '• ⏱ Время действия берется максимальное\n'
        '• 🔄 Можно докупать для увеличения бонуса\n\n'
        '📅 Выберите срок:',
        reply_markup=markup
    )

@cb_donate_router.callback_query(F.data.startswith('donate_auto'))
async def cb_donate_auto(callback: CallbackQuery):
    userid = callback.data.split('_')[-1]
    user = await execute_query_one('SELECT userid FROM stats WHERE userid = ?', (callback.from_user.id,))

    if not user or user[0] != int(userid):
        await callback.answer('⚠️ Это не твое сообщение', show_alert=True)
        return

    await update_data(callback.from_user.username, callback.from_user.id)
    await add_action(callback.from_user.id, 'cb_donate_auto')

    markup = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text='1 день - 25₽', callback_data=f'auto_1day_{callback.from_user.id}')],
        [InlineKeyboardButton(text='3 дня - 60₽', callback_data=f'auto_3days_{callback.from_user.id}')],
        [InlineKeyboardButton(text='1 неделя - 130₽', callback_data=f'auto_1week_{callback.from_user.id}')],
        [InlineKeyboardButton(text='1 месяц - 400₽', callback_data=f'auto_1month_{callback.from_user.id}')],
        [InlineKeyboardButton(text='◀️ Назад', callback_data=f'donate_back_{callback.from_user.id}')]
    ])

    await callback.message.edit_text(
        '🤖 Автоворк и Автоналог\n\n'
        '✨ Возможности:\n\n'
        '• ⚡ Автоматический сбор работы и опыта (/work)\n'
        '• 💰 Автоматическая оплата налогов (/nalog)\n'
        '• 🔄 Работает 24/7 без вашего участия\n'
        '• 📊 Уведомления о выполнении\n\n'
        '📅 Выберите срок:',
        reply_markup=markup
    )

@cb_donate_router.callback_query(F.data.startswith('donate_back'))
async def cb_donate_back(callback: CallbackQuery):
    userid = callback.data.split('_')[-1]
    user = await execute_query_one('SELECT userid FROM stats WHERE userid = ?', (callback.from_user.id,))

    if not user or user[0] != int(userid):
        await callback.answer('⚠️ Это не твое сообщение', show_alert=True)
        return

    markup = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text='👑 PREMIUM Статус', callback_data=f'donate_premium_{callback.from_user.id}')],
        [InlineKeyboardButton(text='👨‍💻 Спонсор клуба', callback_data=f'donate_sponsor_{callback.from_user.id}')],
        [InlineKeyboardButton(text='🤖 Автоматизация', callback_data=f'donate_auto_{callback.from_user.id}')]
    ])

    await callback.message.edit_text(
        '💎 Донат меню\n\n'
        '👑 PREMIUM Статус - увеличение дохода фермы и эксклюзивные возможности\n'
        '👨‍💻 Спонсор клуба - бонус к доходу клуба\n'
        '🤖 Автоматизация - автоворк и автоналог\n\n'
        f'Выберите интересующий вас раздел:',
        reply_markup=markup
    )

# PREMIUM payment handlers
@cb_donate_router.callback_query(F.data.startswith('premium_1day'))
async def cb_premium_1day(callback: CallbackQuery):
    userid = callback.data.split('_')[-1]
    user = await execute_query_one('SELECT userid FROM stats WHERE userid = ?', (callback.from_user.id,))

    if not user or user[0] != int(userid):
        await callback.answer('⚠️ Это не твое сообщение', show_alert=True)
        return

    await update_data(callback.from_user.username, callback.from_user.id)
    await add_action(callback.from_user.id, 'cb_premium_1day')

    payment_url = f"https://yoomoney.ru/quickpay/confirm.xml?receiver=4100118865752483&sum=40&label={uuid.uuid4()}"

    markup = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text='💳 Оплатить', url=payment_url)],
        [InlineKeyboardButton(text='✅ Проверить', callback_data=f'success_{callback.from_user.id}')]
    ])

    await callback.message.edit_text(
        'Оплата 👑 PREMIUM 👑\n\n'
        'Цена: 40 руб.\n'
        'Срок: 1 день\n\n'
        'Оплатите PREMIUM по кнопке ниже, и нажмите "Проверить"',
        reply_markup=markup
    )

    await execute_update('INSERT INTO orders (userid, label, product, amount, days) VALUES (?, ?, ?, ?, ?)',
                 (callback.from_user.id, str(uuid.uuid4()), 'PREMIUM 1 день', 40, 1))

@cb_donate_router.callback_query(F.data.startswith('premium_3days'))
async def cb_premium_3days(callback: CallbackQuery):
    userid = callback.data.split('_')[-1]
    user = await execute_query_one('SELECT userid FROM stats WHERE userid = ?', (callback.from_user.id,))

    if not user or user[0] != int(userid):
        await callback.answer('⚠️ Это не твое сообщение', show_alert=True)
        return

    await update_data(callback.from_user.username, callback.from_user.id)
    await add_action(callback.from_user.id, 'cb_premium_3days')

    payment_url = f"https://yoomoney.ru/quickpay/confirm.xml?receiver=4100118865752483&sum=100&label={uuid.uuid4()}"

    markup = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text='💳 Оплатить', url=payment_url)],
        [InlineKeyboardButton(text='✅ Проверить', callback_data=f'success_{callback.from_user.id}')]
    ])

    await callback.message.edit_text(
        'Оплата 👑 PREMIUM 👑\n\n'
        'Цена: 100 руб.\n'
        'Срок: 3 дня\n\n'
        'Оплатите PREMIUM по кнопке ниже, и нажмите "Проверить"',
        reply_markup=markup
    )

    await execute_update('INSERT INTO orders (userid, label, product, amount, days) VALUES (?, ?, ?, ?, ?)',
                 (callback.from_user.id, str(uuid.uuid4()), 'PREMIUM 3 дня', 100, 3))

@cb_donate_router.callback_query(F.data.startswith('premium_1week'))
async def cb_premium_1week(callback: CallbackQuery):
    userid = callback.data.split('_')[-1]
    user = await execute_query_one('SELECT userid FROM stats WHERE userid = ?', (callback.from_user.id,))

    if not user or user[0] != int(userid):
        await callback.answer('⚠️ Это не твое сообщение', show_alert=True)
        return

    await update_data(callback.from_user.username, callback.from_user.id)
    await add_action(callback.from_user.id, 'cb_premium_1week')

    payment_url = f"https://yoomoney.ru/quickpay/confirm.xml?receiver=4100118865752483&sum=225&label={uuid.uuid4()}"

    markup = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text='💳 Оплатить', url=payment_url)],
        [InlineKeyboardButton(text='✅ Проверить', callback_data=f'success_{callback.from_user.id}')]
    ])

    await callback.message.edit_text(
        'Оплата 👑 PREMIUM 👑\n\n'
        'Цена: 225 руб.\n'
        'Срок: 1 неделя\n\n'
        'Оплатите PREMIUM по кнопке ниже, и нажмите "Проверить"',
        reply_markup=markup
    )

    await execute_update('INSERT INTO orders (userid, label, product, amount, days) VALUES (?, ?, ?, ?, ?)',
                 (callback.from_user.id, str(uuid.uuid4()), 'PREMIUM 1 неделя', 225, 7))

@cb_donate_router.callback_query(F.data.startswith('premium_1month'))
async def cb_premium_1month(callback: CallbackQuery):
    userid = callback.data.split('_')[-1]
    user = await execute_query_one('SELECT userid FROM stats WHERE userid = ?', (callback.from_user.id,))

    if not user or user[0] != int(userid):
        await callback.answer('⚠️ Это не твое сообщение', show_alert=True)
        return

    await update_data(callback.from_user.username, callback.from_user.id)
    await add_action(callback.from_user.id, 'cb_premium_1month')

    payment_url = f"https://yoomoney.ru/quickpay/confirm.xml?receiver=4100118865752483&sum=500&label={uuid.uuid4()}"

    markup = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text='💳 Оплатить', url=payment_url)],
        [InlineKeyboardButton(text='✅ Проверить', callback_data=f'success_{callback.from_user.id}')]
    ])

    await callback.message.edit_text(
        'Оплата 👑 PREMIUM 👑\n\n'
        'Цена: 500 руб.\n'
        'Срок: 1 месяц\n\n'
        'Оплатите PREMIUM по кнопке ниже, и нажмите "Проверить"',
        reply_markup=markup
    )

    await execute_update('INSERT INTO orders (userid, label, product, amount, days) VALUES (?, ?, ?, ?, ?)',
                 (callback.from_user.id, str(uuid.uuid4()), 'PREMIUM 1 месяц', 500, 30))

# SPONSOR payment handlers
@cb_donate_router.callback_query(F.data.startswith('sponsor_1day'))
async def cb_sponsor_1day(callback: CallbackQuery):
    userid = callback.data.split('_')[-1]
    user = await execute_query_one('SELECT userid FROM stats WHERE userid = ?', (callback.from_user.id,))

    if not user or user[0] != int(userid):
        await callback.answer('⚠️ Это не твое сообщение', show_alert=True)
        return

    await update_data(callback.from_user.username, callback.from_user.id)
    await add_action(callback.from_user.id, 'cb_sponsor_1day')

    payment_url = f"https://yoomoney.ru/quickpay/confirm.xml?receiver=4100118865752483&sum=35&label={uuid.uuid4()}"

    markup = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text='💳 Оплатить', url=payment_url)],
        [InlineKeyboardButton(text='✅ Проверить', callback_data=f'success_{callback.from_user.id}')]
    ])

    await callback.message.edit_text(
        'Оплата 👨‍💻 Спонсор клуба\n\n'
        'Цена: 35 руб.\n'
        'Срок: 1 день\n\n'
        'Оплатите Спонсорство по кнопке ниже, и нажмите "Проверить"',
        reply_markup=markup
    )

    await execute_update('INSERT INTO orders (userid, label, product, amount, days) VALUES (?, ?, ?, ?, ?)',
                 (callback.from_user.id, str(uuid.uuid4()), 'Спонсор клуба 1 день', 35, 1))

@cb_donate_router.callback_query(F.data.startswith('sponsor_3days'))
async def cb_sponsor_3days(callback: CallbackQuery):
    userid = callback.data.split('_')[-1]
    user = await execute_query_one('SELECT userid FROM stats WHERE userid = ?', (callback.from_user.id,))

    if not user or user[0] != int(userid):
        await callback.answer('⚠️ Это не твое сообщение', show_alert=True)
        return

    await update_data(callback.from_user.username, callback.from_user.id)
    await add_action(callback.from_user.id, 'cb_sponsor_3days')

    payment_url = f"https://yoomoney.ru/quickpay/confirm.xml?receiver=4100118865752483&sum=75&label={uuid.uuid4()}"

    markup = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text='💳 Оплатить', url=payment_url)],
        [InlineKeyboardButton(text='✅ Проверить', callback_data=f'success_{callback.from_user.id}')]
    ])

    await callback.message.edit_text(
        'Оплата 👨‍💻 Спонсор клуба\n\n'
        'Цена: 75 руб.\n'
        'Срок: 3 дня\n\n'
        'Оплатите Спонсорство по кнопке ниже, и нажмите "Проверить"',
        reply_markup=markup
    )

    await execute_update('INSERT INTO orders (userid, label, product, amount, days) VALUES (?, ?, ?, ?, ?)',
                 (callback.from_user.id, str(uuid.uuid4()), 'Спонсор клуба 3 дня', 75, 3))

@cb_donate_router.callback_query(F.data.startswith('sponsor_1week'))
async def cb_sponsor_1week(callback: CallbackQuery):
    userid = callback.data.split('_')[-1]
    user = await execute_query_one('SELECT userid FROM stats WHERE userid = ?', (callback.from_user.id,))

    if not user or user[0] != int(userid):
        await callback.answer('⚠️ Это не твое сообщение', show_alert=True)
        return

    await update_data(callback.from_user.username, callback.from_user.id)
    await add_action(callback.from_user.id, 'cb_sponsor_1week')

    payment_url = f"https://yoomoney.ru/quickpay/confirm.xml?receiver=4100118865752483&sum=150&label={uuid.uuid4()}"

    markup = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text='💳 Оплатить', url=payment_url)],
        [InlineKeyboardButton(text='✅ Проверить', callback_data=f'success_{callback.from_user.id}')]
    ])

    await callback.message.edit_text(
        'Оплата 👨‍💻 Спонсор клуба\n\n'
        'Цена: 150 руб.\n'
        'Срок: 1 неделя\n\n'
        'Оплатите Спонсорство по кнопке ниже, и нажмите "Проверить"',
        reply_markup=markup
    )

    await execute_update('INSERT INTO orders (userid, label, product, amount, days) VALUES (?, ?, ?, ?, ?)',
                 (callback.from_user.id, str(uuid.uuid4()), 'Спонсор клуба 1 неделя', 150, 7))

@cb_donate_router.callback_query(F.data.startswith('sponsor_1month'))
async def cb_sponsor_1month(callback: CallbackQuery):
    userid = callback.data.split('_')[-1]
    user = await execute_query_one('SELECT userid FROM stats WHERE userid = ?', (callback.from_user.id,))

    if not user or user[0] != int(userid):
        await callback.answer('⚠️ Это не твое сообщение', show_alert=True)
        return

    await update_data(callback.from_user.username, callback.from_user.id)
    await add_action(callback.from_user.id, 'cb_sponsor_1month')

    payment_url = f"https://yoomoney.ru/quickpay/confirm.xml?receiver=4100118865752483&sum=400&label={uuid.uuid4()}"

    markup = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text='💳 Оплатить', url=payment_url)],
        [InlineKeyboardButton(text='✅ Проверить', callback_data=f'success_{callback.from_user.id}')]
    ])

    await callback.message.edit_text(
        'Оплата 👨‍💻 Спонсор клуба\n\n'
        'Цена: 400 руб.\n'
        'Срок: 1 месяц\n\n'
        'Оплатите Спонсорство по кнопке ниже, и нажмите "Проверить"',
        reply_markup=markup
    )

    await execute_update('INSERT INTO orders (userid, label, product, amount, days) VALUES (?, ?, ?, ?, ?)',
                 (callback.from_user.id, str(uuid.uuid4()), 'Спонсор клуба 1 месяц', 400, 30))

# AUTO payment handlers
@cb_donate_router.callback_query(F.data.startswith('auto_1day'))
async def cb_auto_1day(callback: CallbackQuery):
    userid = callback.data.split('_')[-1]
    user = await execute_query_one('SELECT userid FROM stats WHERE userid = ?', (callback.from_user.id,))

    if not user or user[0] != int(userid):
        await callback.answer('⚠️ Это не твое сообщение', show_alert=True)
        return

    await update_data(callback.from_user.username, callback.from_user.id)
    await add_action(callback.from_user.id, 'cb_auto_1day')

    payment_url = f"https://yoomoney.ru/quickpay/confirm.xml?receiver=4100118865752483&sum=25&label={uuid.uuid4()}"

    markup = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text='💳 Оплатить', url=payment_url)],
        [InlineKeyboardButton(text='✅ Проверить', callback_data=f'success_{callback.from_user.id}')]
    ])

    await callback.message.edit_text(
        'Оплата 🤖 Автоматизация\n\n'
        'Цена: 25 руб.\n'
        'Срок: 1 день\n\n'
        'Оплатите Автоматизацию по кнопке ниже, и нажмите "Проверить"',
        reply_markup=markup
    )

    await execute_update('INSERT INTO orders (userid, label, product, amount, days) VALUES (?, ?, ?, ?, ?)',
                 (callback.from_user.id, str(uuid.uuid4()), 'Автоматизация 1 день', 25, 1))

@cb_donate_router.callback_query(F.data.startswith('auto_3days'))
async def cb_auto_3days(callback: CallbackQuery):
    userid = callback.data.split('_')[-1]
    user = await execute_query_one('SELECT userid FROM stats WHERE userid = ?', (callback.from_user.id,))

    if not user or user[0] != int(userid):
        await callback.answer('⚠️ Это не твое сообщение', show_alert=True)
        return

    await update_data(callback.from_user.username, callback.from_user.id)
    await add_action(callback.from_user.id, 'cb_auto_3days')

    payment_url = f"https://yoomoney.ru/quickpay/confirm.xml?receiver=4100118865752483&sum=60&label={uuid.uuid4()}"

    markup = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text='💳 Оплатить', url=payment_url)],
        [InlineKeyboardButton(text='✅ Проверить', callback_data=f'success_{callback.from_user.id}')]
    ])

    await callback.message.edit_text(
        'Оплата 🤖 Автоматизация\n\n'
        'Цена: 60 руб.\n'
        'Срок: 3 дня\n\n'
        'Оплатите Автоматизацию по кнопке ниже, и нажмите "Проверить"',
        reply_markup=markup
    )

    await execute_update('INSERT INTO orders (userid, label, product, amount, days) VALUES (?, ?, ?, ?, ?)',
                 (callback.from_user.id, str(uuid.uuid4()), 'Автоматизация 3 дня', 60, 3))

@cb_donate_router.callback_query(F.data.startswith('auto_1week'))
async def cb_auto_1week(callback: CallbackQuery):
    userid = callback.data.split('_')[-1]
    user = await execute_query_one('SELECT userid FROM stats WHERE userid = ?', (callback.from_user.id,))

    if not user or user[0] != int(userid):
        await callback.answer('⚠️ Это не твое сообщение', show_alert=True)
        return

    await update_data(callback.from_user.username, callback.from_user.id)
    await add_action(callback.from_user.id, 'cb_auto_1week')

    payment_url = f"https://yoomoney.ru/quickpay/confirm.xml?receiver=4100118865752483&sum=130&label={uuid.uuid4()}"

    markup = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text='💳 Оплатить', url=payment_url)],
        [InlineKeyboardButton(text='✅ Проверить', callback_data=f'success_{callback.from_user.id}')]
    ])

    await callback.message.edit_text(
        'Оплата 🤖 Автоматизация\n\n'
        'Цена: 130 руб.\n'
        'Срок: 1 неделя\n\n'
        'Оплатите Автоматизацию по кнопке ниже, и нажмите "Проверить"',
        reply_markup=markup
    )

    await execute_update('INSERT INTO orders (userid, label, product, amount, days) VALUES (?, ?, ?, ?, ?)',
                 (callback.from_user.id, str(uuid.uuid4()), 'Автоматизация 1 неделя', 130, 7))

@cb_donate_router.callback_query(F.data.startswith('auto_1month'))
async def cb_auto_1month(callback: CallbackQuery):
    userid = callback.data.split('_')[-1]
    user = await execute_query_one('SELECT userid FROM stats WHERE userid = ?', (callback.from_user.id,))

    if not user or user[0] != int(userid):
        await callback.answer('⚠️ Это не твое сообщение', show_alert=True)
        return

    await update_data(callback.from_user.username, callback.from_user.id)
    await add_action(callback.from_user.id, 'cb_auto_1month')

    payment_url = f"https://yoomoney.ru/quickpay/confirm.xml?receiver=4100118865752483&sum=400&label={uuid.uuid4()}"

    markup = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text='💳 Оплатить', url=payment_url)],
        [InlineKeyboardButton(text='✅ Проверить', callback_data=f'success_{callback.from_user.id}')]
    ])

    await callback.message.edit_text(
        'Оплата 🤖 Автоматизация\n\n'
        'Цена: 400 руб.\n'
        'Срок: 1 месяц\n\n'
        'Оплатите Автоматизацию по кнопке ниже, и нажмите "Проверить"',
        reply_markup=markup
    )

    await execute_update('INSERT INTO orders (userid, label, product, amount, days) VALUES (?, ?, ?, ?, ?)',
                 (callback.from_user.id, str(uuid.uuid4()), 'Автоматизация 1 месяц', 400, 30))

@cb_donate_router.callback_query(F.data.startswith('donate_1day'))
async def cb_donate_1day(callback: CallbackQuery):
    userid = callback.data.split('_')[-1]
    user = await execute_query_one('SELECT userid FROM stats WHERE userid = ?', (callback.from_user.id,))
    
    if not user or user[0] != int(userid):
        await callback.answer('⚠️ Это не твое сообщение', show_alert=True)
        return
        
    await update_data(callback.from_user.username, callback.from_user.id)
    await add_action(callback.from_user.id, 'cb_donate_1day')
    
    # Simplified payment URL generation (replace with actual YooMoney integration)
    payment_url = f"https://yoomoney.ru/quickpay/confirm.xml?receiver=4100118865752483&sum=20&label={uuid.uuid4()}"
    
    markup = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text='💳 Оплатить', url=payment_url)],
        [InlineKeyboardButton(text='✅ Проверить', callback_data=f'success_{callback.from_user.id}')]
    ])
    
    await callback.message.edit_text(
        'Оплата 👑 PREMIUM 👑\n\n'
        'Цена: 20 руб.\n'
        'Срок: 1 день\n\n'
        'Оплатите PREMIUM по кнопке ниже, и нажмите "Проверить"', 
        reply_markup=markup
    )
    
    # Save order to database
    await execute_update('INSERT INTO orders (userid, label, product, amount, days) VALUES (?, ?, ?, ?, ?)', 
                 (callback.from_user.id, str(uuid.uuid4()), 'PREMIUM 1 день', 20, 1))

@cb_donate_router.callback_query(F.data.startswith('donate_1week'))
async def cb_donate_1week(callback: CallbackQuery):
    userid = callback.data.split('_')[-1]
    user = await execute_query_one('SELECT userid FROM stats WHERE userid = ?', (callback.from_user.id,))
    
    if not user or user[0] != int(userid):
        await callback.answer('⚠️ Это не твое сообщение', show_alert=True)
        return
        
    await update_data(callback.from_user.username, callback.from_user.id)
    await add_action(callback.from_user.id, 'cb_donate_1week')
    
    # Simplified payment URL generation
    payment_url = f"https://yoomoney.ru/quickpay/confirm.xml?receiver=4100118865752483&sum=100&label={uuid.uuid4()}"
    
    markup = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text='💳 Оплатить', url=payment_url)],
        [InlineKeyboardButton(text='✅ Проверить', callback_data=f'success_{callback.from_user.id}')]
    ])
    
    await callback.message.edit_text(
        'Оплата 👑 PREMIUM 👑\n\n'
        'Цена: 100 руб.\n'
        'Срок: 1 неделя\n\n'
        'Оплатите PREMIUM по кнопке ниже, и нажмите "Проверить"', 
        reply_markup=markup
    )
    
    await execute_update('INSERT INTO orders (userid, label, product, amount, days) VALUES (?, ?, ?, ?, ?)', 
                 (callback.from_user.id, str(uuid.uuid4()), 'PREMIUM 1 неделя', 100, 7))

@cb_donate_router.callback_query(F.data.startswith('donate_1month'))
async def cb_donate_1month(callback: CallbackQuery):
    userid = callback.data.split('_')[-1]
    user = await execute_query_one('SELECT userid FROM stats WHERE userid = ?', (callback.from_user.id,))
    
    if not user or user[0] != int(userid):
        await callback.answer('⚠️ Это не твое сообщение', show_alert=True)
        return
        
    await update_data(callback.from_user.username, callback.from_user.id)
    await add_action(callback.from_user.id, 'cb_donate_1month')
    
    # Simplified payment URL generation
    payment_url = f"https://yoomoney.ru/quickpay/confirm.xml?receiver=4100118865752483&sum=300&label={uuid.uuid4()}"
    
    markup = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text='💳 Оплатить', url=payment_url)],
        [InlineKeyboardButton(text='✅ Проверить', callback_data=f'success_{callback.from_user.id}')]
    ])
    
    await callback.message.edit_text(
        'Оплата 👑 PREMIUM 👑\n\n'
        'Цена: 300 руб.\n'
        'Срок: 1 месяц\n\n'
        'Оплатите PREMIUM по кнопке ниже, и нажмите "Проверить"', 
        reply_markup=markup
    )
    
    await execute_update('INSERT INTO orders (userid, label, product, amount, days) VALUES (?, ?, ?, ?, ?)', 
                 (callback.from_user.id, str(uuid.uuid4()), 'PREMIUM 1 месяц', 300, 30))

# ===== TEXT MESSAGE HANDLERS =====
@cmd_user_router.message(F.text == '👤 Профиль')
async def msg_profile(message: Message):
    await cmd_profile(message)

@cmd_user_router.message(F.text == '🖥 ПК в наличии')
async def msg_my_pcs(message: Message):
    await cmd_my_pcs(message)

@cmd_user_router.message(F.text == '🏆 Топ')
async def msg_top(message: Message):
    await cmd_top(message)

@cmd_user_router.message(F.text == '👑 Донат')
async def msg_donate(message: Message):
    await cmd_donate(message)



# ===== MAIN FUNCTION =====
async def calculate_income():
    """Начисление дохода пользователям каждые 10 минут с учетом ВСЕХ бонусов включая экспансии"""
    conn = await Database.get_connection()
    
    try:
        # Очищаем истекшие события
        await cleanup_expired_events()
        
        # Получаем всех пользователей
        users = await execute_query('SELECT userid, income, network, taxes, room, bal, premium FROM stats')
        
        for user in users:
            user_id = user[0]
            base_income = Decimal(str(user[1]))  # Чистый доход от компьютеров
            network_id = user[2]
            taxes_debt = Decimal(str(user[3]))
            room_level = user[4]
            current_balance = Decimal(str(user[5]))
            premium = user[6]
            
            # Если базовый доход 0, пропускаем пользователя
            if base_income == 0:
                continue
            
            # Начинаем с базового дохода
            final_income = base_income

            # === БОНУС ЭКСПАНСИИ (только к чистому доходу) ===
            expansion_bonus_percent = await get_expansion_bonus(user_id)
            if expansion_bonus_percent > 0:
                expansion_bonus = base_income * Decimal(str(expansion_bonus_percent))
                final_income += expansion_bonus

            # === ДОБАВЛЯЕМ БОНУС РЕПУТАЦИИ ===
            rep_income_bonus, rep_tax_reduction = await get_reputation_bonuses(user_id)
            if rep_income_bonus > 0:
                reputation_bonus = base_income * Decimal(str(rep_income_bonus))
                final_income += reputation_bonus

            # === ДОБАВЛЯЕМ СОЦИАЛЬНЫЕ БОНУСЫ ===
            social_bonus_percent = await get_social_bonus(user_id)
            if social_bonus_percent > 0:
                social_bonus = base_income * Decimal(str(social_bonus_percent))
                final_income += social_bonus

            # Проверяем PREMIUM статус
            if premium:
                premium_date = safe_parse_datetime(premium)
                if premium_date and premium_date > datetime.datetime.now():
                    premium_bonus = base_income * Decimal('0.35')  # +35% за премиум
                    final_income += premium_bonus

            # Применяем улучшения
            upgrades = await execute_query_one(
                'SELECT upgrade_internet, upgrade_devices, upgrade_service FROM stats WHERE userid = ?',
                (user_id,)
            )

            if upgrades:
                upgrade_bonus = sum(upgrades) / 100.0
                final_income += base_income * Decimal(str(upgrade_bonus))

            # Применяем активную рекламу
            user_ad = await execute_query_one(
                'SELECT num, percent, dt FROM ads WHERE userid = ? ORDER BY dt DESC LIMIT 1',
                (user_id,)
            )

            if user_ad:
                for ad in ads:
                    if user_ad[0] == ad[0]:
                        ad_dt = safe_parse_datetime(user_ad[2])
                        if ad_dt and ad_dt + datetime.timedelta(hours=ad[4]) > datetime.datetime.now():
                            ad_bonus = base_income * Decimal(str(user_ad[1])) / Decimal('100')
                            final_income += ad_bonus
                        break

            # Бонус от событий
            event_bonus = await get_event_bonus(user_id)
            if event_bonus > 0:
                event_income = base_income * Decimal(str(event_bonus))
                final_income += event_income

            # В конце применяем бустер дохода (income booster) ко ВСЕМУ итоговому доходу
            final_income = await apply_boosters(user_id, final_income)
            
            income_to_add = final_income
            
            # Получаем максимальный налог для текущего уровня комнаты с учетом экспансии
            max_tax = Decimal('0')
            expansion_level = await get_expansion_level(user_id)
            
            if expansion_level == 0:
                # Базовые налоги
                for tax in taxes:
                    if room_level == tax[0]:
                        max_tax = Decimal(str(tax[1]))
                        break
            else:
                # Налоги для экспансий
                expansion_taxes = get_taxes_for_expansion(expansion_level)
                for tax in expansion_taxes:
                    if room_level == tax[0]:
                        max_tax = Decimal(str(tax[1]))
                        break
            
            # ИСПРАВЛЕНИЕ: Если максимальный налог 0 (для уровня 1), не блокируем доход
            if max_tax == 0:
                # Начисляем доход без проверки налогов
                new_balance = current_balance + income_to_add
                await execute_update('UPDATE stats SET bal = ? WHERE userid = ?', 
                             (float(new_balance), user_id))
            elif taxes_debt >= max_tax:
                income_to_add = Decimal('0')
                try:
                    await bot.send_message(
                        user_id, 
                        f'⚠️ ВНИМАНИЕ! Ваш доход заморожен из-за налоговой задолженности!\n'
                        f'Налоги: {format_number_short(taxes_debt, True)}$/{format_number_short(max_tax, True)}$ (МАКСИМУМ)\n'
                        f'Оплатите налоги: /pay_taxes'
                    )
                except Exception:
                    pass
            else:
                new_balance = current_balance + income_to_add
                await execute_update('UPDATE stats SET bal = ? WHERE userid = ?', 
                             (float(new_balance), user_id))
            
            if current_balance + income_to_add > Decimal(str(user[5])):
                await execute_update('UPDATE stats SET max_bal = ? WHERE userid = ?', 
                             (float(current_balance + income_to_add), user_id))
            
            if network_id and base_income > 0:
                await execute_update('UPDATE networks SET income = income + ? WHERE owner_id = ?', 
                             (float(base_income), network_id))
                await execute_update('UPDATE stats SET net_inc = net_inc + ? WHERE userid = ?', 
                             (float(base_income), user_id))
            
            if income_to_add > 0:
                await execute_update('UPDATE stats SET all_wallet = all_wallet + ? WHERE userid = ?', 
                             (float(income_to_add), user_id))
            
        logger.info("10-minute income calculation with expansion bonuses completed successfully")
        
    except Exception as e:
        logger.error(f"Error in calculate_income: {e}")
        
        
@cmd_admin_router.message(Command('add_booster'))
async def cmd_add_booster(message: Message):
    """Добавить бустер пользователю"""
    if message.from_user.id not in ADMIN:
        await message.answer('❌ Недостаточно прав')
        return
        
    text_parts = message.text.split(' ')
    
    if len(text_parts) != 4:
        await message.answer(
            '⚠️ Используйте: /add_booster [type] [id] [days]\n\n'
            '📊 Типы бустеров:\n'
            '• income - +25% к доходу на N дней\n'
            '• auto - автоматическая оплата налогов и работа на N дней\n'
            '• premium - 👑 PREMIUM статус (+35% к доходу) на N дней\n\n'
            '*Примеры:*\n'
            '`/add_booster income 5929120983 7`\n'
            '`/add_booster auto 5929120983 30`\n'
            '`/add_booster premium 5929120983 30`'
        )
        return

    booster_type = text_parts[1].lower()
    target_user_id = int(text_parts[2])
    days = int(text_parts[3])

    if days <= 0:
        await message.answer('❌ Количество дней должно быть больше 0')
        return

    if booster_type not in ['income', 'auto', 'premium']:
        await message.answer('❌ Неверный тип бустера. Доступно: income, auto, premium')
        return
        
    try:
        # Проверяем существование пользователя
        user = await execute_query_one(
            'SELECT name FROM stats WHERE userid = ?', 
            (target_user_id,)
        )
        
        if not user:
            await message.answer('❌ Пользователь не найден')
            return
            
        user_name = user[0]
        
        # Добавляем бустер
        success = await add_booster_to_user(target_user_id, booster_type, days)
        
        if not success:
            await message.answer('❌ Ошибка при добавлении бустера')
            return
        
        # Получаем информацию о бустере
        booster_info = BOOSTER_TYPES[booster_type]
        end_date = datetime.datetime.now() + datetime.timedelta(days=days)
        
        response_text = (
            f'✅ <b>Бустер добавлен!</b>\n\n'
            f'👤 Пользователь: <b>{user_name}</b>\n'
            f'🆔 ID: <code>{target_user_id}</code>\n'
            f'🎯 Тип: <b>{booster_info["name"]}</b>\n'
            f'📅 Действует до: <code>{end_date.strftime("%d.%m.%Y %H:%M")}</code>'
        )
        
        await message.answer(response_text, parse_mode='HTML')
        
        # Уведомляем пользователя
        try:
            user_notification = (
                f'🎉 <b>Вам выдан бустер!</b>\n\n'
                f'✨ {booster_info["name"]}\n'
                f'📅 Действует до: {end_date.strftime("%d.%m.%Y %H:%M")}\n\n'
            )
            
            if booster_type == "income":
                user_notification += f'💡 Теперь вы получаете +25% к доходу!'
            elif booster_type == "auto":
                user_notification += f'💡 Теперь налоги оплачиваются автоматически и работа выполняется каждый час!'
            elif booster_type == "premium":
                user_notification += (
                    f'💡 PREMIUM бонусы:\n'
                    f'• 🎛 +35% к доходу фермы\n'
                    f'• 🎁 Ежедневный бонус раз в 12 часов\n'
                    f'• ⚡ Приоритетная поддержка'
                )

            await bot.send_message(target_user_id, user_notification, parse_mode='HTML')
        except Exception as e:
            logger.warning(f"Could not notify user {target_user_id}: {e}")
        
        logger.info(f"Admin {message.from_user.id} added {booster_type} booster to user {target_user_id} for {days} days")
        
    except Exception as e:
        logger.error(f"Error adding booster: {e}")
        await message.answer('❌ Ошибка при добавлении бустера')

# ===== КОМАНДА ДЛЯ ПРОСМОТРА БУСТЕРОВ =====
@cmd_user_router.message(Command('boosters'))
async def cmd_boosters(message: Message):
    """Показать активные бустеры"""
    user = await execute_query_one('SELECT name FROM stats WHERE userid = ?', (message.from_user.id,))
    if not user:
        await message.answer('Сначала зарегистрируйтесь - /start')
        return
        
    await update_data(message.from_user.username, message.from_user.id)
    await add_action(message.from_user.id, 'cmd_boosters')
    
    active_boosters = await get_active_boosters(message.from_user.id)
    
    if not active_boosters:
        await message.answer(
            '🎯 <b>У вас нет активных бустеров</b>\n\n'
            'Доступные бустеры:\n'
            '• 📈 Бустер дохода - +25% к доходу\n'
            '• 🤖 Автоматизация - авто-налоги и работа\n'
            '• 👑 PREMIUM - +35% к доходу и другие бонусы\n\n'
            '💡 Бустеры можно получить от администраторов',
            parse_mode='HTML'
        )
        return
    
    text = '🎯 <b>Ваши активные бустеры:</b>\n\n'
    
    for booster_type, booster_data in active_boosters.items():
        booster_info = BOOSTER_TYPES[booster_type]
        end_date = booster_data["end_date"]
        days_left = booster_data["days_left"]
        
        text += (
            f'✨ <b>{booster_info["name"]}</b>\n'
            f'📅 Действует до: <code>{end_date.strftime("%d.%m.%Y %H:%M")}</code>\n'
            f'⏰ Осталось дней: <b>{days_left}</b>\n\n'
        )
    
    await message.answer(text, parse_mode='HTML')
        
        
async def process_auto_boosters():
    """Обработка автоматических бустеров (налоги и работа)"""
    try:
        now = datetime.datetime.now()
        
        # Находим пользователей с активным auto_booster
        users_with_auto = await execute_query(
            'SELECT userid, taxes, bal FROM stats WHERE auto_booster_end > ?',
            (now,)
        )
        
        for user in users_with_auto:
            user_id = user[0]
            taxes = Decimal(str(user[1]))
            balance = Decimal(str(user[2]))
            
            # Автоматически оплачиваем налоги если есть средства
            if taxes > 0 and balance >= taxes:
                await execute_update(
                    'UPDATE stats SET bal = bal - ?, taxes = 0 WHERE userid = ?',
                    (float(taxes), user_id)
                )
                logger.info(f"Auto-paid taxes for user {user_id}: {taxes}$")
            
            # Автоматически выполняем работу
            exp, last_work = await get_work_stats(user_id)
            if last_work:
                next_work = last_work + datetime.timedelta(hours=1)
                if datetime.datetime.now() >= next_work:
                    # Находим максимальную доступную работу
                    max_job = None
                    for job in WORK_JOBS:
                        if job['min_exp'] <= exp < job['max_exp']:
                            max_job = job
                            break
                    
                    if max_job:
                        # Выполняем работу
                        reward = max_job['reward']
                        await execute_update(
                            'UPDATE stats SET bal = bal + ? WHERE userid = ?',
                            (reward, user_id)
                        )
                        await execute_update('''
                            UPDATE user_work_stats 
                            SET exp = exp + 1, last_work = ?, total_earned = total_earned + ?
                            WHERE user_id = ?
                        ''', (datetime.datetime.now().isoformat(), reward, user_id))
                        
                        # Добавляем репутацию за автоматическую работу
                        rep_points = max_job['id']
                        await add_reputation(user_id, rep_points, "auto_work")
                        
                        logger.info(f"Auto-work completed for user {user_id}: {max_job['name']} (+{reward}$)")
        
    except Exception as e:
        logger.error(f"Error processing auto boosters: {e}")

        
        
async def schedule_boosters_processing():
    """Планировщик для обработки автоматических бустеров"""
    while True:
        try:
            now = datetime.datetime.now()
            
            # Очистка истекших бустеров каждые 5 минут
            if now.minute % 5 == 0:
                await cleanup_expired_boosters()
            
            # Обработка автоматических бустеров каждые 30 минут
            if now.minute % 30 == 0:
                await process_auto_boosters()
            
            await asyncio.sleep(60)  # Проверяем каждую минуту
            
        except Exception as e:
            logger.error(f"Error in schedule_boosters_processing: {e}")
            await asyncio.sleep(60)
        
async def calculate_taxes():
    """Начисление налогов с учетом бонуса репутации"""
    conn = await Database.get_connection()
    
    try:
        users = await execute_query('SELECT userid, income, taxes, room FROM stats WHERE income > 0')
        
        for user in users:
            user_id = user[0]
            user_income = Decimal(str(user[1]))
            current_taxes = Decimal(str(user[2]))
            room_level = user[3]
            
            # Если доход 0, не начисляем налоги
            if user_income == 0:
                continue
            
            # === ДОБАВЛЯЕМ БОНУС РЕПУТАЦИИ ДЛЯ НАЛОГОВ ===
            _, rep_tax_reduction = await get_reputation_bonuses(user_id)
            
            # Базовый налог 25%, уменьшаем на бонус репутации
            tax_rate = Decimal('0.25') - Decimal(str(rep_tax_reduction))
            tax_amount = user_income * tax_rate
            
            # Получаем максимальный налог для текущего уровня комнаты с учетом экспансии
            max_tax = Decimal('0')
            expansion_level = await get_expansion_level(user_id)
            
            if expansion_level == 0:
                # Базовые налоги
                for tax in taxes:
                    if room_level == tax[0]:
                        max_tax = Decimal(str(tax[1]))
                        break
            else:
                # Налоги для экспансий
                expansion_taxes = get_taxes_for_expansion(expansion_level)
                for tax in expansion_taxes:
                    if room_level == tax[0]:
                        max_tax = Decimal(str(tax[1]))
                        break
            
            # Если максимальный налог 0 (для уровня 1), не начисляем налоги
            if max_tax == 0:
                continue
            
            new_taxes = current_taxes + tax_amount
            
            # Если новые налоги превышают максимум, устанавливаем максимум
            if new_taxes > max_tax:
                new_taxes = max_tax
            
            await execute_update('UPDATE stats SET taxes = ? WHERE userid = ?', 
                         (float(new_taxes), user_id))
            
        logger.info("Hourly tax calculation with reputation bonus completed successfully")
        
    except Exception as e:
        logger.error(f"Error in calculate_taxes: {e}")
        
        
@cmd_admin_router.message(Command('clear_all_taxes'))
async def cmd_clear_all_taxes(message: Message):
    """Очистить налоги у всех пользователей"""
    if message.from_user.id not in ADMIN:
        await message.answer('❌ Недостаточно прав')
        return
        
    try:
        # Получаем статистику перед очисткой
        total_users = await execute_query('SELECT COUNT(*) FROM stats')
        users_with_taxes = await execute_query('SELECT COUNT(*) FROM stats WHERE taxes > 0')
        total_taxes = await execute_query('SELECT SUM(taxes) FROM stats WHERE taxes > 0')
        
        users_count = total_users[0][0] if total_users else 0
        taxed_users = users_with_taxes[0][0] if users_with_taxes else 0
        taxes_sum = total_taxes[0][0] if total_taxes and total_taxes[0][0] else 0
        
        if taxed_users == 0:
            await message.answer('ℹ️ Нет пользователей с налоговой задолженностью')
            return
        
        # Очищаем налоги у всех пользователей
        await execute_update('UPDATE stats SET taxes = 0 WHERE taxes > 0')
        
        await message.answer(
            f'✅ <b>Налоги успешно очищены!</b>\n\n'
            f'👥 Всего пользователей: <b>{users_count}</b>\n'
            f'💰 Очищено налогов у: <b>{taxed_users}</b> пользователей\n'
            f'💸 Общая сумма очищенных налогов: <b>{format_number_short(taxes_sum, True)}$</b>\n'
            f'⏰ Время: <code>{datetime.datetime.now().strftime("%d.%m.%Y %H:%M")}</code>',
            parse_mode='HTML'
        )
        
        logger.info(f"Admin {message.from_user.id} cleared all taxes: {taxed_users} users, {taxes_sum}$")
        
    except Exception as e:
        logger.error(f"Error in clear_all_taxes: {e}")
        await message.answer('❌ Ошибка при очистке налогов')        
                        
async def schedule_income_calculation():
    """Планировщик для раздельного начисления дохода и налогов"""
    last_income_minute = -1
    last_tax_hour = -1

    while True:
        try:
            now = datetime.datetime.now()
            current_minute = now.minute
            current_hour = now.hour

            # Начисление дохода каждые 10 минут
            if current_minute % 10 == 0 and last_income_minute != current_minute:
                logger.info("Starting 10-minute income calculation...")
                await calculate_income()
                logger.info("10-minute income calculation completed")
                last_income_minute = current_minute

            # Начисление налогов каждый ровный час (минута 00)
            if current_minute == 0 and last_tax_hour != current_hour:
                logger.info("Starting hourly tax calculation...")
                await calculate_taxes()
                logger.info("Hourly tax calculation completed")
                last_tax_hour = current_hour

            await asyncio.sleep(30)  # Проверять каждые 30 секунд

        except Exception as e:
            logger.error(f"Error in schedule_income_calculation: {e}")
            await asyncio.sleep(60)

async def reset_weekly_income():
    """Сброс недельного дохода франшиз (каждое воскресенье в 19:00)"""
    while True:
        try:
            now = datetime.datetime.now()
            
            # Вычисляем время до следующего воскресенья 19:00 по МСК
            msk_offset = datetime.timedelta(hours=3)
            now_msk = now + msk_offset
            
            # Определяем день недели (0 - понедельник, 6 - воскресенье)
            current_weekday = now_msk.weekday()
            
            # Вычисляем дни до воскресенья
            days_until_sunday = (6 - current_weekday) % 7
            
            # Если сегодня воскресенье и время меньше 19:00, используем сегодня
            if current_weekday == 6 and now_msk.hour < 19:
                days_until_sunday = 0
            # Если сегодня воскресенье и время больше 19:00, ждем следующее воскресенье
            elif current_weekday == 6 and now_msk.hour >= 19:
                days_until_sunday = 7
            
            next_sunday = now_msk.replace(hour=19, minute=0, second=0, microsecond=0) + datetime.timedelta(days=days_until_sunday)
            
            # Конвертируем обратно в UTC для вычисления времени ожидания
            next_sunday_utc = next_sunday - msk_offset
            wait_seconds = (next_sunday_utc - now).total_seconds()
            
            logger.info(f"Next franchise income reset scheduled for: {next_sunday} (MSK), waiting {wait_seconds} seconds")
            
            if wait_seconds > 0:
                await asyncio.sleep(wait_seconds)
            
            # Сбрасываем доход франшиз
            await execute_update('UPDATE networks SET income = 0')
            await execute_update('UPDATE stats SET net_inc = 0')
            
            # Награждаем топовые франшизы PREMIUM
            top_franchises = await execute_query('SELECT owner_id, name FROM networks WHERE income > 0 ORDER BY income DESC LIMIT 5')
            
            rewarded_users = set()
            
            for i, franchise in enumerate(top_franchises):
                franchise_id = franchise[0]
                franchise_name = franchise[1]
                
                # Награждаем владельца франшизы (только если не был награжден ранее)
                if franchise_id not in rewarded_users:
                    current_premium = await execute_query('SELECT premium FROM stats WHERE userid = ?', (franchise_id,))
                    if current_premium:
                        premium_date = safe_parse_datetime(current_premium[0][0])
                        if premium_date and premium_date > datetime.datetime.now():
                            new_premium = premium_date + datetime.timedelta(days=7)
                        else:
                            new_premium = datetime.datetime.now() + datetime.timedelta(days=7)
                        
                        await execute_update('UPDATE stats SET premium = ? WHERE userid = ?', 
                                     (new_premium, franchise_id))
                        rewarded_users.add(franchise_id)
                        
                        try:
                            await bot.send_message(
                                franchise_id,
                                f'🎉 Поздравляем! Ваша франшиза "{franchise_name}" вошла в топ-5 и получает PREMIUM на 7 дней!'
                            )
                        except Exception as e:
                            logger.warning(f"Could not notify franchise owner {franchise_id}: {e}")
                
                # Для топ-5 награждаем 2 случайных участника
                if i < 5:
                    top_members = await execute_query('''
                        SELECT userid FROM stats 
                        WHERE network = ? AND userid != ?
                        ORDER BY net_inc DESC 
                        LIMIT 5
                    ''', (franchise_id, franchise_id))
                    
                    if len(top_members) >= 2:
                        random_members = random.sample([m[0] for m in top_members], 2)
                        for member_id in random_members:
                            if member_id not in rewarded_users:
                                member_premium = await execute_query('SELECT premium FROM stats WHERE userid = ?', (member_id,))
                                if member_premium:
                                    member_premium_date = safe_parse_datetime(member_premium[0][0])
                                    if member_premium_date and member_premium_date > datetime.datetime.now():
                                        new_premium = member_premium_date + datetime.timedelta(days=7)
                                    else:
                                        new_premium = datetime.datetime.now() + datetime.timedelta(days=7)
                                    
                                    await execute_update('UPDATE stats SET premium = ? WHERE userid = ?', 
                                                 (new_premium, member_id))
                                    rewarded_users.add(member_id)
                                    
                                    try:
                                        await bot.send_message(
                                            member_id,
                                            f'🎉 Поздравляем! Вы получили PREMIUM на 7 дней за активность в топовой франшизе "{franchise_name}"!'
                                        )
                                    except Exception as e:
                                        logger.warning(f"Could not notify franchise member {member_id}: {e}")
            
            logger.info(f"Weekly franchise income reset completed. Rewarded {len(rewarded_users)} users")
            
        except Exception as e:
            logger.error(f"Error in reset_weekly_income: {e}")
            await asyncio.sleep(3600)  # Ждем час при ошибке

async def reset_daily_bonus():
    """Ежедневный сброс бонуса в 00:00 по МСК"""
    while True:
        try:
            now = datetime.datetime.now()
            
            # Вычисляем время до следующего 00:00 по МСК (UTC+3)
            msk_offset = datetime.timedelta(hours=3)
            now_msk = now + msk_offset
            next_reset = (now_msk + datetime.timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
            wait_seconds = (next_reset - now_msk).total_seconds()
            
            logger.info(f"Next daily bonus reset scheduled for: {next_reset}")
            await asyncio.sleep(wait_seconds)
            
            # Сбрасываем бонус для всех пользователей
            await execute_update('UPDATE stats SET bonus = 1')
            logger.info("Daily bonus reset completed")
            
        except Exception as e:
            logger.error(f"Error in reset_daily_bonus: {e}")
            await asyncio.sleep(3600)


async def get_active_event(user_id: int):
    """Получить активное событие пользователя"""
    result = await execute_query_one(
        'SELECT event_type, bonus_percent, end_time FROM user_events WHERE user_id = ? AND end_time > ?',
        (user_id, datetime.datetime.now())
    )
    return result

async def create_random_event(user_id: int):
    """Создать случайное событие для пользователя"""
    # Выбираем событие по весам
    total_weight = sum(event["weight"] for event in EVENTS)
    rnd = random.uniform(0, total_weight)
    
    current_weight = 0
    selected_event = None
    
    for event in EVENTS:
        current_weight += event["weight"]
        if rnd <= current_weight:
            selected_event = event
            break
    
    if not selected_event:
        selected_event = EVENTS[0]  # fallback
    
    # Генерируем случайные значения
    bonus_percent = random.randint(selected_event["min_percent"], selected_event["max_percent"])
    duration_hours = random.randint(selected_event["min_hours"], selected_event["max_hours"])
    
    end_time = datetime.datetime.now() + datetime.timedelta(hours=duration_hours)
    
    # Сохраняем в базу
    await execute_update(
        'INSERT OR REPLACE INTO user_events (user_id, event_type, bonus_percent, end_time) VALUES (?, ?, ?, ?)',
        (user_id, selected_event["type"], bonus_percent, end_time)
    )
    
    return {
        "type": selected_event["type"],
        "name": selected_event["name"],
        "bonus_percent": bonus_percent,
        "end_time": end_time,
        "duration_hours": duration_hours
    }

async def cleanup_expired_events():
    """Очистить истекшие события"""
    await execute_update('DELETE FROM user_events WHERE end_time <= ?', (datetime.datetime.now(),))

async def get_event_bonus(user_id: int) -> float:
    """Получить текущий бонус от события в процентах"""
    event = await get_active_event(user_id)
    if event:
        return event[1] / 100.0  # Convert percent to multiplier
    return 0.0




def format_time(seconds):
    """Форматирование времени в читаемый вид"""
    if seconds < 60:
        return f"{int(seconds)} сек"
    elif seconds < 3600:
        return f"{int(seconds // 60)} мин {int(seconds % 60)} сек"
    elif seconds < 86400:
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        return f"{hours} ч {minutes} мин"
    else:
        days = int(seconds // 86400)
        hours = int((seconds % 86400) // 3600)
        return f"{days} д {hours} ч"
        
@cmd_admin_router.message(Command('create_events_all'))
async def cmd_create_events_all(message: Message):
    """Создать события для всех активных пользователей"""
    if message.from_user.id not in ADMIN:
        await message.answer('❌ Недостаточно прав')
        return
        
    try:
        # Получаем всех активных пользователей
        active_users = await execute_query('''
            SELECT DISTINCT s.userid, s.name
            FROM stats s 
            JOIN actions a ON s.userid = a.userid 
            WHERE a.dt >= ?
        ''', (datetime.datetime.now() - datetime.timedelta(days=7),))
        
        if not active_users:
            await message.answer('❌ Нет активных пользователей')
            return
        
        total_users = len(active_users)
        events_created = 0
        users_with_events = 0
        failed_users = 0
        
        # Сначала отправляем сообщение о начале процесса
        progress_msg = await message.answer(f"🔄 Создание событий для {total_users} пользователей...\n0/{total_users}")
        
        for i, user in enumerate(active_users, 1):
            user_id = user[0]
            user_name = user[1] or f"ID{user_id}"
            
            # Обновляем прогресс каждые 10 пользователей
            if i % 10 == 0 or i == total_users:
                try:
                    await progress_msg.edit_text(
                        f"🔄 Создание событий...\n{i}/{total_users} ({events_created} создано)"
                    )
                except:
                    pass
            
            # Проверяем, нет ли уже активного события
            active_event = await get_active_event(user_id)
            if active_event:
                users_with_events += 1
                continue
            
            try:
                # Создаем случайное событие
                event = await create_random_event(user_id)
                events_created += 1
                
                # Отправляем уведомление пользователю
                try:
                    event_message = (
                        f"🎉 {event['name']} посетил ваш ПК Клуб!\n"
                        f"🔥 Вы получили: +{event['bonus_percent']}% к доходу\n"
                        f"⏰ Срок действия: {event['duration_hours']} часов"
                    )
                    await bot.send_message(user_id, event_message)
                except Exception as e:
                    logger.error(f"Failed to send event notification to {user_id}: {e}")
                    # Не считаем это ошибкой - событие создано, просто уведомление не отправлено
                    
            except Exception as e:
                logger.error(f"Failed to create event for user {user_id}: {e}")
                failed_users += 1
        
        # Итоговый отчет
        report = (
            f"✅ <b>Создание событий завершено!</b>\n\n"
            f"📊 <b>Результаты:</b>\n"
            f"• Всего пользователей: {total_users}\n"
            f"• Создано событий: {events_created}\n"
            f"• Уже имели события: {users_with_events}\n"
            f"• Ошибок: {failed_users}\n\n"
            f"⏰ Время: {datetime.datetime.now().strftime('%d.%m.%Y %H:%M')}"
        )
        
        await progress_msg.edit_text(report, parse_mode='HTML')
        
    except Exception as e:
        logger.error(f"Error in create_events_all: {e}")
        await message.answer(f'❌ Ошибка при создании событий: {str(e)}')        

async def random_events_scheduler():
    """Планировщик случайных событий - создает события для всех активных пользователей раз в сутки"""
    while True:
        try:
            # Ждем до следующего дня в 12:00 по МСК
            now = datetime.datetime.now()
            msk_offset = datetime.timedelta(hours=3)
            now_msk = now + msk_offset
            
            # Следующий день в 12:00 по МСК
            next_day = (now_msk + datetime.timedelta(days=1)).replace(hour=12, minute=0, second=0, microsecond=0)
            wait_seconds = (next_day - now_msk).total_seconds()
            
            logger.info(f"Next random events scheduled for: {next_day} (MSK)")
            await asyncio.sleep(wait_seconds)
            
            # Получаем всех активных пользователей (были активны последние 7 дней)
            active_users = await execute_query('''
                SELECT DISTINCT s.userid 
                FROM stats s 
                JOIN actions a ON s.userid = a.userid 
                WHERE a.dt >= ?
            ''', (datetime.datetime.now() - datetime.timedelta(days=7),))
            
            events_created = 0
            users_with_active_events = 0
            
            for user in active_users:
                user_id = user[0]
                
                # Проверяем, нет ли уже активного события
                active_event = await get_active_event(user_id)
                if active_event:
                    users_with_active_events += 1
                    continue
                
                # Создаем случайное событие
                event = await create_random_event(user_id)
                events_created += 1
                
                # Отправляем уведомление пользователю
                try:
                    event_message = (
                        f"🎉 {event['name']} посетил ваш ПК Клуб!\n"
                        f"🔥 Вы получили: +{event['bonus_percent']}% к доходу\n"
                        f"⏰ Срок действия: {event['duration_hours']} часов"
                    )
                    await bot.send_message(user_id, event_message)
                except Exception as e:
                    logger.error(f"Failed to send event notification to {user_id}: {e}")
            
            logger.info(f"Random events created: {events_created} for {len(active_users)} active users (already had events: {users_with_active_events})")
            
        except Exception as e:
            logger.error(f"Error in random_events_scheduler: {e}")
            await asyncio.sleep(3600)  # Ждем час при ошибке

@cmd_admin_router.message(Command('remove_all_premium'))
async def cmd_remove_all_premium(message: Message):
    if message.from_user.id not in ADMIN:
        await message.answer('❌ Недостаточно прав')
        return
        
    try:
        # Получаем количество пользователей с активным премиумом
        active_premium = await execute_query(
            'SELECT COUNT(*) FROM stats WHERE premium > ?', 
            (datetime.datetime.now(),)
        )
        
        if active_premium[0][0] == 0:
            await message.answer('ℹ️ Нет пользователей с активным премиумом')
            return
            
        # Создаем клавиатуру для подтверждения
        markup = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text='✅ Да, удалить', callback_data=f'confirm_remove_premium_{message.from_user.id}'),
                InlineKeyboardButton(text='❌ Отмена', callback_data=f'cancel_remove_premium_{message.from_user.id}')
            ]
        ])
        
        await message.answer(
            f'⚠️ <b>ВНИМАНИЕ!</b>\n\n'
            f'Вы собираетесь удалить премиум у <b>{active_premium[0][0]}</b> пользователей.\n\n'
            f'Это действие нельзя отменить!\n'
            f'Подтвердите удаление:',
            parse_mode='HTML',
            reply_markup=markup
        )
        
    except Exception as e:
        logger.error(f"Error in remove_all_premium: {e}")
        await message.answer('❌ Ошибка при получении статистики')

# Обработчик подтверждения удаления
@cb_admin_router.callback_query(F.data.startswith('confirm_remove_premium_'))
async def cb_confirm_remove_premium(callback: CallbackQuery):
    userid = callback.data.split('_')[-1]
    
    if not userid.isdigit() or callback.from_user.id != int(userid):
        await callback.answer('⚠️ Это не твое сообщение', show_alert=True)
        return
        
    if callback.from_user.id not in ADMIN:
        await callback.answer('❌ Недостаточно прав', show_alert=True)
        return
        
    try:
        # Удаляем премиум у всех пользователей (устанавливаем текущую дату)
        result = await execute_update(
            'UPDATE stats SET premium = ? WHERE premium > ?', 
            (datetime.datetime.now(), datetime.datetime.now())
        )
        
        # Получаем количество обновленных записей
        updated_count = await execute_query(
            'SELECT changes()'
        )
        
        await callback.message.edit_text(
            f'✅ <b>Премиум успешно удален!</b>\n\n'
            f'Затронуто пользователей: <b>{updated_count[0][0] if updated_count else "N/A"}</b>\n'
            f'Время: <code>{datetime.datetime.now().strftime("%d.%m.%Y %H:%M")}</code>',
            parse_mode='HTML'
        )
        
        logger.info(f"Admin {callback.from_user.id} removed premium from all users")
        
    except Exception as e:
        logger.error(f"Error removing all premium: {e}")
        await callback.message.edit_text('❌ Ошибка при удалении премиума')

# Обработчик отмены удаления
@cb_admin_router.callback_query(F.data.startswith('cancel_remove_premium_'))
async def cb_cancel_remove_premium(callback: CallbackQuery):
    userid = callback.data.split('_')[-1]
    
    if not userid.isdigit() or callback.from_user.id != int(userid):
        await callback.answer('⚠️ Это не твое сообщение', show_alert=True)
        return
        
    await callback.message.edit_text('❌ Удаление премиума отменено') 


async def check_all_social_bonuses():
    """Проверяет подписки пользователей, которым ранее был выдан бонус"""
    while True:
        try:
            logger.info("Starting social bonus check...")
            
            # Получаем только пользователей, у которых есть запись о бонусах
            users_with_bonus = await execute_query('''
            SELECT user_id FROM user_social_bonus 
            WHERE channel_subscribed = TRUE OR chat_subscribed = TRUE OR bio_checked = TRUE
            ''')
            
            # Проверяем только первых 10 пользователей (чтобы не перегружать API)
            users_to_check = users_with_bonus[:10]
            
            for (user_id,) in users_to_check:
                try:
                    await update_all_bonuses(user_id)
                    await asyncio.sleep(0.1)  # Небольшая задержка между запросами
                except Exception as e:
                    logger.error(f"Error checking bonuses for user {user_id}: {e}")
                    continue
                    
            logger.info(f"Social bonus check completed for {len(users_to_check)} users")
            
        except Exception as e:
            logger.error(f"Error in social bonus check: {e}")
        
        # Ждем 10 минут до следующей проверки
        await asyncio.sleep(600)

# Запуск фоновой задачи проверки бонусов
async def start_social_bonus_checker():
    """Запускает фоновую задачу проверки бонусов"""
    asyncio.create_task(check_all_social_bonuses())

async def check_and_fix_database():
    """Проверяет и исправляет структуру базы данных"""
    conn = await Database.get_connection()
    
    try:
        # Проверяем существование всех необходимых колонок
        columns_to_check = [
            'expansion_level',
            'income_booster_end', 
            'auto_booster_end'
        ]
        
        for column in columns_to_check:
            try:
                await conn.execute(f'SELECT {column} FROM stats LIMIT 1')
                logger.info(f"Column {column} exists")
            except Exception:
                logger.info(f"Adding missing column: {column}")
                if column == 'expansion_level':
                    await conn.execute(f'ALTER TABLE stats ADD COLUMN {column} INTEGER DEFAULT 0')
                else:
                    await conn.execute(f'ALTER TABLE stats ADD COLUMN {column} TIMESTAMP')
        
        await conn.commit()
        logger.info("Database structure check completed successfully")
        
    except Exception as e:
        logger.error(f"Error checking database structure: {e}")


def format_time(seconds):
    """Форматирование времени в читаемый вид"""
    if seconds < 60:
        return f"{int(seconds)} сек"
    elif seconds < 3600:
        return f"{int(seconds // 60)} мин {int(seconds % 60)} сек"
    elif seconds < 86400:
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        return f"{hours} ч {minutes} мин"
    else:
        days = int(seconds // 86400)
        hours = int((seconds % 86400) // 3600)
        return f"{days} д {hours} ч"
        
@cmd_admin_router.message(Command('create_events_all'))
async def cmd_create_events_all(message: Message):
    """Создать события для всех активных пользователей"""
    if message.from_user.id not in ADMIN:
        await message.answer('❌ Недостаточно прав')
        return
        
    try:
        # Получаем всех активных пользователей
        active_users = await execute_query('''
            SELECT DISTINCT s.userid, s.name
            FROM stats s 
            JOIN actions a ON s.userid = a.userid 
            WHERE a.dt >= ?
        ''', (datetime.datetime.now() - datetime.timedelta(days=7),))
        
        if not active_users:
            await message.answer('❌ Нет активных пользователей')
            return
        
        total_users = len(active_users)
        events_created = 0
        users_with_events = 0
        failed_users = 0
        
        # Сначала отправляем сообщение о начале процесса
        progress_msg = await message.answer(f"🔄 Создание событий для {total_users} пользователей...\n0/{total_users}")
        
        for i, user in enumerate(active_users, 1):
            user_id = user[0]
            user_name = user[1] or f"ID{user_id}"
            
            # Обновляем прогресс каждые 10 пользователей
            if i % 10 == 0 or i == total_users:
                try:
                    await progress_msg.edit_text(
                        f"🔄 Создание событий...\n{i}/{total_users} ({events_created} создано)"
                    )
                except:
                    pass
            
            # Проверяем, нет ли уже активного события
            active_event = await get_active_event(user_id)
            if active_event:
                users_with_events += 1
                continue
            
            try:
                # Создаем случайное событие
                event = await create_random_event(user_id)
                events_created += 1
                
                # Отправляем уведомление пользователю
                try:
                    event_message = (
                        f"🎉 {event['name']} посетил ваш ПК Клуб!\n"
                        f"🔥 Вы получили: +{event['bonus_percent']}% к доходу\n"
                        f"⏰ Срок действия: {event['duration_hours']} часов"
                    )
                    await bot.send_message(user_id, event_message)
                except Exception as e:
                    logger.error(f"Failed to send event notification to {user_id}: {e}")
                    # Не считаем это ошибкой - событие создано, просто уведомление не отправлено
                    
            except Exception as e:
                logger.error(f"Failed to create event for user {user_id}: {e}")
                failed_users += 1
        
        # Итоговый отчет
        report = (
            f"✅ <b>Создание событий завершено!</b>\n\n"
            f"📊 <b>Результаты:</b>\n"
            f"• Всего пользователей: {total_users}\n"
            f"• Создано событий: {events_created}\n"
            f"• Уже имели события: {users_with_events}\n"
            f"• Ошибок: {failed_users}\n\n"
            f"⏰ Время: {datetime.datetime.now().strftime('%d.%m.%Y %H:%M')}"
        )
        
        await progress_msg.edit_text(report, parse_mode='HTML')
        
    except Exception as e:
        logger.error(f"Error in create_events_all: {e}")
        await message.answer(f'❌ Ошибка при создании событий: {str(e)}')        

async def random_events_scheduler():
    """Планировщик случайных событий - создает события для всех активных пользователей раз в сутки"""
    while True:
        try:
            # Ждем до следующего дня в 12:00 по МСК
            now = datetime.datetime.now()
            msk_offset = datetime.timedelta(hours=3)
            now_msk = now + msk_offset
            
            # Следующий день в 12:00 по МСК
            next_day = (now_msk + datetime.timedelta(days=1)).replace(hour=12, minute=0, second=0, microsecond=0)
            wait_seconds = (next_day - now_msk).total_seconds()
            
            logger.info(f"Next random events scheduled for: {next_day} (MSK)")
            await asyncio.sleep(wait_seconds)
            
            # Получаем всех активных пользователей (были активны последние 7 дней)
            active_users = await execute_query('''
                SELECT DISTINCT s.userid 
                FROM stats s 
                JOIN actions a ON s.userid = a.userid 
                WHERE a.dt >= ?
            ''', (datetime.datetime.now() - datetime.timedelta(days=7),))
            
            events_created = 0
            users_with_active_events = 0
            
            for user in active_users:
                user_id = user[0]
                
                # Проверяем, нет ли уже активного события
                active_event = await get_active_event(user_id)
                if active_event:
                    users_with_active_events += 1
                    continue
                
                # Создаем случайное событие
                event = await create_random_event(user_id)
                events_created += 1
                
                # Отправляем уведомление пользователю
                try:
                    event_message = (
                        f"🎉 {event['name']} посетил ваш ПК Клуб!\n"
                        f"🔥 Вы получили: +{event['bonus_percent']}% к доходу\n"
                        f"⏰ Срок действия: {event['duration_hours']} часов"
                    )
                    await bot.send_message(user_id, event_message)
                except Exception as e:
                    logger.error(f"Failed to send event notification to {user_id}: {e}")
            
            logger.info(f"Random events created: {events_created} for {len(active_users)} active users (already had events: {users_with_active_events})")
            
        except Exception as e:
            logger.error(f"Error in random_events_scheduler: {e}")
            await asyncio.sleep(3600)  # Ждем час при ошибке

@cmd_admin_router.message(Command('remove_all_premium'))
async def cmd_remove_all_premium(message: Message):
    if message.from_user.id not in ADMIN:
        await message.answer('❌ Недостаточно прав')
        return
        
    try:
        # Получаем количество пользователей с активным премиумом
        active_premium = await execute_query(
            'SELECT COUNT(*) FROM stats WHERE premium > ?', 
            (datetime.datetime.now(),)
        )
        
        if active_premium[0][0] == 0:
            await message.answer('ℹ️ Нет пользователей с активным премиумом')
            return
            
        # Создаем клавиатуру для подтверждения
        markup = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text='✅ Да, удалить', callback_data=f'confirm_remove_premium_{message.from_user.id}'),
                InlineKeyboardButton(text='❌ Отмена', callback_data=f'cancel_remove_premium_{message.from_user.id}')
            ]
        ])
        
        await message.answer(
            f'⚠️ <b>ВНИМАНИЕ!</b>\n\n'
            f'Вы собираетесь удалить премиум у <b>{active_premium[0][0]}</b> пользователей.\n\n'
            f'Это действие нельзя отменить!\n'
            f'Подтвердите удаление:',
            parse_mode='HTML',
            reply_markup=markup
        )
        
    except Exception as e:
        logger.error(f"Error in remove_all_premium: {e}")
        await message.answer('❌ Ошибка при получении статистики')

# Обработчик подтверждения удаления
@cb_admin_router.callback_query(F.data.startswith('confirm_remove_premium_'))
async def cb_confirm_remove_premium(callback: CallbackQuery):
    userid = callback.data.split('_')[-1]
    
    if not userid.isdigit() or callback.from_user.id != int(userid):
        await callback.answer('⚠️ Это не твое сообщение', show_alert=True)
        return
        
    if callback.from_user.id not in ADMIN:
        await callback.answer('❌ Недостаточно прав', show_alert=True)
        return
        
    try:
        # Удаляем премиум у всех пользователей (устанавливаем текущую дату)
        result = await execute_update(
            'UPDATE stats SET premium = ? WHERE premium > ?', 
            (datetime.datetime.now(), datetime.datetime.now())
        )
        
        # Получаем количество обновленных записей
        updated_count = await execute_query(
            'SELECT changes()'
        )
        
        await callback.message.edit_text(
            f'✅ <b>Премиум успешно удален!</b>\n\n'
            f'Затронуто пользователей: <b>{updated_count[0][0] if updated_count else "N/A"}</b>\n'
            f'Время: <code>{datetime.datetime.now().strftime("%d.%m.%Y %H:%M")}</code>',
            parse_mode='HTML'
        )
        
        logger.info(f"Admin {callback.from_user.id} removed premium from all users")
        
    except Exception as e:
        logger.error(f"Error removing all premium: {e}")
        await callback.message.edit_text('❌ Ошибка при удалении премиума')

# Обработчик отмены удаления
@cb_admin_router.callback_query(F.data.startswith('cancel_remove_premium_'))
async def cb_cancel_remove_premium(callback: CallbackQuery):
    userid = callback.data.split('_')[-1]
    
    if not userid.isdigit() or callback.from_user.id != int(userid):
        await callback.answer('⚠️ Это не твое сообщение', show_alert=True)
        return
        
    await callback.message.edit_text('❌ Удаление премиума отменено') 


async def check_all_social_bonuses():
    """Проверяет подписки пользователей, которым ранее был выдан бонус"""
    while True:
        try:
            logger.info("Starting social bonus check...")
            
            # Получаем только пользователей, у которых есть запись о бонусах
            users_with_bonus = await execute_query('''
            SELECT user_id FROM user_social_bonus 
            WHERE channel_subscribed = TRUE OR chat_subscribed = TRUE OR bio_checked = TRUE
            ''')
            
            # Проверяем только первых 10 пользователей (чтобы не перегружать API)
            users_to_check = users_with_bonus[:10]
            
            for (user_id,) in users_to_check:
                try:
                    await update_all_bonuses(user_id)
                    await asyncio.sleep(0.1)  # Небольшая задержка между запросами
                except Exception as e:
                    logger.error(f"Error checking bonuses for user {user_id}: {e}")
                    continue
                    
            logger.info(f"Social bonus check completed for {len(users_to_check)} users")
            
        except Exception as e:
            logger.error(f"Error in social bonus check: {e}")
        
        # Ждем 10 минут до следующей проверки
        await asyncio.sleep(600)

# Запуск фоновой задачи проверки бонусов
async def start_social_bonus_checker():
    """Запускает фоновую задачу проверки бонусов"""
    asyncio.create_task(check_all_social_bonuses())

async def check_and_fix_database():
    """Проверяет и исправляет структуру базы данных"""
    conn = await Database.get_connection()
    
    try:
        # Проверяем существование всех необходимых колонок
        columns_to_check = [
            'expansion_level',
            'income_booster_end', 
            'auto_booster_end'
        ]
        
        for column in columns_to_check:
            try:
                await conn.execute(f'SELECT {column} FROM stats LIMIT 1')
                logger.info(f"Column {column} exists")
            except Exception:
                logger.info(f"Adding missing column: {column}")
                if column == 'expansion_level':
                    await conn.execute(f'ALTER TABLE stats ADD COLUMN {column} INTEGER DEFAULT 0')
                else:
                    await conn.execute(f'ALTER TABLE stats ADD COLUMN {column} TIMESTAMP')
        
        await conn.commit()
        logger.info("Database structure check completed successfully")
        
    except Exception as e:
        logger.error(f"Error checking database structure: {e}")

async def reset_weekly_income():
    """Сбросить доход франшиз и участников франшиз"""
    try:
        # Сбрасываем доход франшиз
        await execute_update('UPDATE networks SET income = 0')
        
        # Сбрасываем доход участников франшиз (net_inc)
        await execute_update('UPDATE stats SET net_inc = 0 WHERE network IS NOT NULL')
        
        logger.info("Weekly income reset successfully")
        return True
    except Exception as e:
        logger.error(f"Error resetting weekly income: {e}")
        return False

async def calculate_weekly_stats():
    """Рассчитать статистику за неделю"""
    try:
        # Топ 10 франшиз по доходу за неделю (до сброса!)
        top_franchises = await execute_query(
            'SELECT name, income, owner_id FROM networks WHERE owner_id != ? ORDER BY income DESC LIMIT 10',
            (ADMIN[0],)
        )
        
        # Общая статистика
        total_users = await execute_query_one('SELECT COUNT(*) FROM stats')
        total_franchises = await execute_query_one('SELECT COUNT(*) FROM networks WHERE owner_id != ?', (ADMIN[0],))
        
        # Суммарный доход всех франшиз
        total_franchise_income = await execute_query_one('SELECT SUM(income) FROM networks WHERE owner_id != ?', (ADMIN[0],))
        
        # Лучшие участники каждой франшизы в топ-10
        top_members_by_franchise = []
        for franchise in top_franchises:
            franchise_id = franchise[2]
            top_member = await execute_query_one(
                'SELECT name, net_inc FROM stats WHERE network = ? ORDER BY net_inc DESC LIMIT 1',
                (franchise_id,)
            )
            if top_member:
                top_members_by_franchise.append({
                    'franchise_id': franchise_id,
                    'franchise_name': franchise[0],
                    'member_name': top_member[0],
                    'member_income': top_member[1]
                })
        
        return {
            'top_franchises': top_franchises,
            'top_members_by_franchise': top_members_by_franchise,
            'total_users': total_users[0] if total_users else 0,
            'total_franchises': total_franchises[0] if total_franchises else 0,
            'total_franchise_income': total_franchise_income[0] if total_franchise_income else 0,
            'week_end': datetime.datetime.now().strftime('%d.%m.%Y')
        }
    except Exception as e:
        logger.error(f"Error calculating weekly stats: {e}")
        return None

async def give_weekly_premium(user_id: int, days: int):
    """Выдать премиум победителям недели"""
    try:
        user = await execute_query_one(
            'SELECT name, premium FROM stats WHERE userid = ?', 
            (user_id,)
        )
        
        if not user:
            return False
        
        user_name = user[0]
        current_premium = user[1]
        
        # Рассчитываем новую дату премиума
        new_premium_date = datetime.datetime.now() + datetime.timedelta(days=days)
        
        # Если у пользователя уже есть активный премиум, продлеваем его
        if current_premium:
            current_premium_date = safe_parse_datetime(current_premium)
            if current_premium_date and current_premium_date > datetime.datetime.now():
                new_premium_date = current_premium_date + datetime.timedelta(days=days)
        
        # Выдаем/продлеваем премиум
        await execute_update(
            'UPDATE stats SET premium = ? WHERE userid = ?', 
            (new_premium_date, user_id)
        )
        
        # Уведомляем пользователя
        try:
            await bot.send_message(
                user_id,
                f'🎉 <b>Поздравляем! Вы получили PREMIUM!</b>\n\n'
                f'🏆 Вы вошли в топ франшиз за неделю!\n'
                f'⏰ Срок: <b>{days}</b> дней\n'
                f'📅 Действует до: <code>{new_premium_date.strftime("%d.%m.%Y %H:%M")}</code>\n\n'
                f'✨ Теперь вы получаете +50% к доходу!',
                parse_mode='HTML'
            )
        except Exception:
            pass
        
        logger.info(f"Premium given to user {user_id} for {days} days")
        return True
        
    except Exception as e:
        logger.error(f"Error giving weekly premium: {e}")
        return False

async def create_weekly_promo():
    """Создать промокод после итогов недели"""
    try:
        # Случайное количество часов дохода фермы (1-6 часов)
        hours = random.randint(1, 6)
        
        # Случайное количество активаций (50-150)
        max_activations = random.randint(50, 150)
        
        # Генерируем код
        alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"  # Без похожих символов
        promo_code = ''.join(random.choice(alphabet) for _ in range(8))
        
        # Сохраняем промокод
        await execute_update(
            'INSERT INTO promos (name, use_max, reward, quantity) VALUES (?, ?, ?, ?)',
            (promo_code, max_activations, 'income', hours)
        )
        
        logger.info(f"Weekly promo created: {promo_code} for {hours} hours, {max_activations} activations")
        return promo_code, hours, max_activations
        
    except Exception as e:
        logger.error(f"Error creating weekly promo: {e}")
        return None, 0, 0

async def post_weekly_results():
    """Опубликовать итоги недели в канал и создать промокод"""
    try:
        # 1. Получаем статистику до сброса
        stats = await calculate_weekly_stats()
        if not stats:
            logger.error("Failed to calculate weekly stats")
            return False
        
        # 2. Определяем победителей и выдаем PREMIUM
        winners = []
        used_positions = set()
        
        # Гарантированные победители: 8-е место
        if len(stats['top_franchises']) >= 8:
            franchise = stats['top_franchises'][7]  # 8-е место (индекс 7)
            days = random.randint(3, 7)
            success = await give_weekly_premium(franchise[2], days)
            if success:
                winners.append({
                    'position': 8,
                    'franchise_name': franchise[0],
                    'franchise_id': franchise[2],
                    'days': days
                })
                used_positions.add(7)
        
        # Случайные 2 победителя из оставшихся позиций 4-10 (кроме 8-го)
        available_positions = [i for i in range(3, 10) if i != 7 and i < len(stats['top_franchises'])]
        
        if len(available_positions) >= 2:
            random_positions = random.sample(available_positions, 2)
            for pos in random_positions:
                franchise = stats['top_franchises'][pos]
                days = random.randint(2, 5)
                success = await give_weekly_premium(franchise[2], days)
                if success:
                    winners.append({
                        'position': pos + 1,
                        'franchise_name': franchise[0],
                        'franchise_id': franchise[2],
                        'days': days
                    })
                    used_positions.add(pos)
        
        # 3. Сбрасываем доход франшиз и участников
        await reset_weekly_income()
        
        # 4. Создаем промокод
        promo_code, promo_hours, promo_activations = await create_weekly_promo()
        
        # 5. Формируем сообщение для канала
        text = f"🏆 <b>ИТОГИ НЕДЕЛИ ({stats['week_end']})</b>\n\n"
        
        # Общая статистика
        text += f"📊 <b>Общая статистика:</b>\n"
        text += f"👥 Всего игроков: {stats['total_users']}\n"
        text += f"🌐 Всего франшиз: {stats['total_franchises']}\n"
        text += f"💰 Суммарный доход франшиз: {format_number_short(stats['total_franchise_income'], True)}$\n\n"
        
        # Топ франшиз
        text += "🏅 <b>ТОП-10 ФРАНШИЗ:</b>\n"
        for i, franchise in enumerate(stats['top_franchises'][:10], 1):
            franchise_name = franchise[0] if franchise[0] else "Без названия"
            franchise_income = franchise[1]
            
            medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else f"{i}."
            
            # Добавляем метку PREMIUM для победителей
            is_winner = (i-1) in used_positions
            winner_mark = " 🎁" if is_winner else ""
            
            text += f"{medal} <b>{franchise_name}</b>{winner_mark}\n"
            text += f"   💰 {format_number_short(franchise_income, True)}$\n"
            
            # Добавляем лучшего участника
            for member_info in stats['top_members_by_franchise']:
                if member_info['franchise_id'] == franchise[2]:
                    text += f"   👤 Лучший: {member_info['member_name']} ({format_number_short(member_info['member_income'], True)}$)\n"
                    break
            
            text += "\n"
        
        # Информация о победителях
        if winners:
            text += "🎉 <b>ПОБЕДИТЕЛИ (PREMIUM):</b>\n"
            for winner in winners:
                text += f"🏆 {winner['position']} место: {winner['franchise_name']} (+{winner['days']} дней)\n"
            text += "\n"
        
        # Промокод
        if promo_code:
            text += f"🎁 <b>ЕЖЕНЕДЕЛЬНЫЙ ПРОМОКОД:</b>\n"
            text += f"🔑 Код: <code>{promo_code}</code>\n"
            text += f"💰 Награда: Доход фермы за {promo_hours} часов\n"
            text += f"👥 Активаций: {promo_activations}\n"
            text += f"📝 Активировать: /promo {promo_code}\n\n"
        
        # Правила на следующую неделю
        text += "📢 <b>ПРАВИЛА НА СЛЕДУЩУЮ НЕДЕЛЮ:</b>\n"
        text += "• 8-е место получает PREMIUM гарантированно\n"
        text += "• +2 случайные франшизы из топ-10\n"
        text += "• Доход франшиз обнуляется каждую неделю\n"
        text += "• Новый промокод после каждого топа\n\n"
        
        text += "⏰ <b>Следующие итоги:</b> Воскресенье, 18:00 по МСК\n"
        text += "🔥 Участвуйте и побеждайте!"
        
        # 6. Публикуем в канал
        await bot.send_message(
            CHANNEL_ID,
            text,
            parse_mode='HTML'
        )
        
        logger.info("Weekly results posted successfully")
        return True
        
    except Exception as e:
        logger.error(f"Error posting weekly results: {e}")
        return False

# ===== ПЛАНИРОВЩИК ИТОГОВ НЕДЕЛИ =====

async def schedule_weekly_results():
    """Планировщик для публикации итогов недели каждое воскресенье в 18:00 по Москве

    Расписание:
    - 18:00 - Выдача премиума топ-10 чатам
    - 18:01 - Сброс топа чатов и сбора
    - 18:05 - Генерация промокодов
    """
    logger.info("Weekly results scheduler started (3-phase schedule)")

    while True:
        try:
            now = datetime.datetime.now()

            # Определяем московское время (UTC+3)
            moscow_time = now + datetime.timedelta(hours=3)

            # Проверяем, воскресенье ли сегодня (6 - воскресенье в Python)
            if moscow_time.weekday() == 6:  # Воскресенье
                # 18:00 - Выдача премиума топ-10 чатам
                if moscow_time.hour == 18 and moscow_time.minute == 0:
                    logger.info("Sunday 18:00 Moscow time - awarding premium to top franchises!")

                    # Получаем статистику ДО сброса
                    stats = await calculate_weekly_stats()

                    if stats:
                        # Выдаем премиум топ-10
                        winners = []
                        used_positions = set()

                        # Гарантированные победители: 8-е место
                        if len(stats['top_franchises']) >= 8:
                            franchise = stats['top_franchises'][7]  # 8-е место (индекс 7)
                            days = random.randint(3, 7)
                            success = await give_weekly_premium(franchise[2], days)
                            if success:
                                winners.append({
                                    'position': 8,
                                    'franchise_name': franchise[0],
                                    'franchise_id': franchise[2],
                                    'days': days
                                })
                                used_positions.add(7)

                        # Случайные 2 победителя из оставшихся позиций 4-10 (кроме 8-го)
                        available_positions = [i for i in range(3, 10) if i != 7 and i < len(stats['top_franchises'])]

                        if len(available_positions) >= 2:
                            random_positions = random.sample(available_positions, 2)
                            for pos in random_positions:
                                franchise = stats['top_franchises'][pos]
                                days = random.randint(2, 5)
                                success = await give_weekly_premium(franchise[2], days)
                                if success:
                                    winners.append({
                                        'position': pos + 1,
                                        'franchise_name': franchise[0],
                                        'franchise_id': franchise[2],
                                        'days': days
                                    })
                                    used_positions.add(pos)

                        # Отправляем уведомление админам о выдаче премиума
                        for admin_id in ADMIN:
                            try:
                                text = "🏆 <b>ПРЕМИУМ ВЫДАН!</b>\n\n"
                                for winner in winners:
                                    text += f"• {winner['position']} место: {winner['franchise_name']} (+{winner['days']} дней)\n"
                                await bot.send_message(admin_id, text, parse_mode='HTML')
                            except Exception as e:
                                logger.error(f"Error sending premium notification to admin {admin_id}: {e}")

                        logger.info(f"Premium awarded to {len(winners)} franchises")

                    # Ждем 61 секунду до следующей фазы
                    await asyncio.sleep(61)

                # 18:01 - Сброс топа чатов и сбора
                elif moscow_time.hour == 18 and moscow_time.minute == 1:
                    logger.info("Sunday 18:01 Moscow time - resetting weekly income!")

                    # Сбрасываем доход франшиз
                    success = await reset_weekly_income()

                    if success:
                        logger.info("Weekly income reset successfully")
                        # Уведомляем админов
                        for admin_id in ADMIN:
                            try:
                                await bot.send_message(
                                    admin_id,
                                    "♻️ <b>СБРОС ТОПА</b>\n\nДоход франшиз и участников обнулен",
                                    parse_mode='HTML'
                                )
                            except Exception as e:
                                logger.error(f"Error sending reset notification to admin {admin_id}: {e}")
                    else:
                        logger.error("Failed to reset weekly income")

                    # Ждем 4 минуты до следующей фазы
                    await asyncio.sleep(240)

                # 18:05 - Генерация промокодов
                elif moscow_time.hour == 18 and moscow_time.minute == 5:
                    logger.info("Sunday 18:05 Moscow time - generating weekly promo!")

                    # Создаем промокод
                    promo_code, promo_hours, promo_activations = await create_weekly_promo()

                    if promo_code:
                        logger.info(f"Weekly promo created: {promo_code}")

                        # Отправляем промокод админам
                        for admin_id in ADMIN:
                            try:
                                text = (
                                    f"🎁 <b>ПРОМОКОД СОЗДАН!</b>\n\n"
                                    f"🔑 Код: <code>{promo_code}</code>\n"
                                    f"💰 Награда: {promo_hours} часов дохода\n"
                                    f"👥 Активаций: {promo_activations}\n\n"
                                    f"Опубликуй в канале!"
                                )
                                await bot.send_message(admin_id, text, parse_mode='HTML')
                            except Exception as e:
                                logger.error(f"Error sending promo to admin {admin_id}: {e}")
                    else:
                        logger.error("Failed to create weekly promo")

                    # Ждем до конца дня, чтобы не запускать повторно
                    await asyncio.sleep(24 * 3600)
                else:
                    # Ждем 1 минуту до следующей проверки
                    await asyncio.sleep(60)
            else:
                # Не воскресенье - ждем 1 час
                await asyncio.sleep(3600)

        except Exception as e:
            logger.error(f"Error in schedule_weekly_results: {e}")
            await asyncio.sleep(300)  # Ждем 5 минут при ошибке

BOT_START_TIME = datetime.datetime.now()
async def main():
    """Главная функция бота"""
    # Инициализируем базу данных
    await init_db()
    print("Database initialized successfully")
    
    # Устанавливаем время старта бота
    bot.start_time = datetime.datetime.now()
    
    # Включаем все роутеры
    routers = [
        fsm_router,
        callback_router,
        cmd_user_router,
        cmd_upgrades_router,
        cmd_games_router,
        cmd_franchise_router,
        cmd_economy_router,
        cmd_admin_router,
        cb_network_router,
        cb_economy_router,
        cb_donate_router,
        cb_games_router,
        cb_admin_router
    ]
    
    for router in routers:
        dp.include_router(router)
    
    # Запускаем фоновые задачи
    asyncio.create_task(schedule_income_calculation())
    asyncio.create_task(reset_daily_bonus())
    asyncio.create_task(random_events_scheduler())
    asyncio.create_task(start_social_bonus_checker())
    asyncio.create_task(schedule_boosters_processing())
    
    # ЗАПУСКАЕМ ПЛАНИРОВЩИК ИТОГОВ НЕДЕЛИ
    asyncio.create_task(schedule_weekly_results())
    
    # Start polling
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot, polling_timeout=20)
    
if __name__ == '__main__':
    try:
        print("Starting PC Club Bot...")
        print("Weekly results will be posted every Sunday at 18:00 Moscow time")
        asyncio.run(main())
    except KeyboardInterrupt:
        print('Bot stopped by user')
    except Exception as e:
        print(f'Error: {e}')
    finally:
        # Close database connection
        asyncio.run(Database.close())