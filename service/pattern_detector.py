from pandas import Series
import talib , sys , json , pandas , datetime

sys.path.append('')
sys.path.append('utils/')

from kafka_utils import consume

# -------------------------------------------------------------------------------------

opens = {}
highs = {}
lows = {}
closes = {}

# -------------------------------------------------------------------------------------

def detect():

    for message in consume():

        data = json.loads(message.value)

        date = datetime.datetime.fromtimestamp(data['k']['t']/1000)

        opens[date] = float(data['k']['o'])
        highs[date] = float(data['k']['h'])
        lows[date] = float(data['k']['l'])
        closes[date] = float(data['k']['c'])

        # -------------------------------------------------------------------------------------

        two_crows = talib.CDL2CROWS(pandas.Series(opens) , pandas.Series(highs) , pandas.Series(lows) , pandas.Series(closes))
        if (two_crows[-1] != 0):
            print("{} - two crows pattern detected at -> {}".format(data['s'] , date))

        # -------------------------------------------------------------------------------------

        three_black_crows = talib.CDL3BLACKCROWS(pandas.Series(opens) , pandas.Series(highs) , pandas.Series(lows) , pandas.Series(closes))
        if (three_black_crows[-1] != 0):
            print("{} - three black crows pattern detected at -> {}".format(data['s'] , date))

        # -------------------------------------------------------------------------------------

        three_inside = talib.CDL3INSIDE(pandas.Series(opens) , pandas.Series(highs) , pandas.Series(lows) , pandas.Series(closes))
        if (three_inside[-1] != 0):
            print("{} - three inside pattern detected at -> {}".format(data['s'] , date))

        # -------------------------------------------------------------------------------------

        three_line_strike = talib.CDL3LINESTRIKE(pandas.Series(opens) , pandas.Series(highs) , pandas.Series(lows) , pandas.Series(closes))
        if (three_line_strike[-1] != 0):
            print("{} - three line strike pattern detected at -> {}".format(data['s'] , date))

        # -------------------------------------------------------------------------------------

        three_outside = talib.CDL3OUTSIDE(pandas.Series(opens) , pandas.Series(highs) , pandas.Series(lows) , pandas.Series(closes))
        if (three_outside[-1] != 0):
            print("{} - three outside pattern detected at -> {}".format(data['s'] , date))

        # -------------------------------------------------------------------------------------

        abandoned_baby = talib.CDLABANDONEDBABY(pandas.Series(opens) , pandas.Series(highs) , pandas.Series(lows) , pandas.Series(closes))
        if (abandoned_baby[-1] != 0):
            print("{} - abandoned baby pattern detected at -> {}".format(data['s'] , date))

        # -------------------------------------------------------------------------------------

        advance_block = talib.CDLADVANCEBLOCK(pandas.Series(opens) , pandas.Series(highs) , pandas.Series(lows) , pandas.Series(closes))
        if (advance_block[-1] != 0):
            print("{} - advance block pattern detected at -> {}".format(data['s'] , date))

        # -------------------------------------------------------------------------------------

        belt_hold = talib.CDLBELTHOLD(pandas.Series(opens) , pandas.Series(highs) , pandas.Series(lows) , pandas.Series(closes))
        if (belt_hold[-1] != 0):
            print("{} - belt hold pattern detected at -> {}".format(data['s'] , date))

        # -------------------------------------------------------------------------------------

        break_away = talib.CDLBREAKAWAY(pandas.Series(opens) , pandas.Series(highs) , pandas.Series(lows) , pandas.Series(closes))
        if (break_away[-1] != 0):
            print("{} - break away pattern detected at -> {}".format(data['s'] , date))

        # -------------------------------------------------------------------------------------

        closing_marubozu = talib.CDLCLOSINGMARUBOZU(pandas.Series(opens) , pandas.Series(highs) , pandas.Series(lows) , pandas.Series(closes))
        if (closing_marubozu[-1] != 0):
            print("{} - closing marubozu pattern detected at -> {}".format(data['s'] , date))

        # -------------------------------------------------------------------------------------

        concealing_baby_swallow = talib.CDLCONCEALBABYSWALL(pandas.Series(opens) , pandas.Series(highs) , pandas.Series(lows) , pandas.Series(closes))
        if (concealing_baby_swallow[-1] != 0):
            print("{} - concealing baby swallow pattern detected at -> {}".format(data['s'] , date))

        # -------------------------------------------------------------------------------------

        counter_attack = talib.CDLCOUNTERATTACK(pandas.Series(opens) , pandas.Series(highs) , pandas.Series(lows) , pandas.Series(closes))
        if (counter_attack[-1] != 0):
            print("{} - counter attack pattern detected at -> {}".format(data['s'] , date))

        # -------------------------------------------------------------------------------------

        dark_cloud_cover = talib.CDLDARKCLOUDCOVER(pandas.Series(opens) , pandas.Series(highs) , pandas.Series(lows) , pandas.Series(closes))
        if (dark_cloud_cover[-1] != 0):
            print("{} - dark cloud cover pattern detected at -> {}".format(data['s'] , date))

        # -------------------------------------------------------------------------------------

        doji = talib.CDLDOJI(pandas.Series(opens) , pandas.Series(highs) , pandas.Series(lows) , pandas.Series(closes))
        if (doji[-1] != 0):
            print("{} - doji pattern detected at -> {}".format(data['s'] , date))

        # -------------------------------------------------------------------------------------

        doji_star = talib.CDLDOJISTAR(pandas.Series(opens) , pandas.Series(highs) , pandas.Series(lows) , pandas.Series(closes))
        if (doji_star[-1] != 0):
            print("{} - doji star pattern detected at -> {}".format(data['s'] , date))

        # -------------------------------------------------------------------------------------

        dragonfly_doji = talib.CDLDRAGONFLYDOJI(pandas.Series(opens) , pandas.Series(highs) , pandas.Series(lows) , pandas.Series(closes))
        if (dragonfly_doji[-1] != 0):
            print("{} - dragonfly doji pattern detected at -> {}".format(data['s'] , date))

        # -------------------------------------------------------------------------------------

        engulfing = talib.CDLENGULFING(pandas.Series(opens) , pandas.Series(highs) , pandas.Series(lows) , pandas.Series(closes))
        if (engulfing[-1] != 0):
            print("{} - engulfing pattern detected at -> {}".format(data['s'] , date))

        # -------------------------------------------------------------------------------------

        evening_doji_start = talib.CDLEVENINGDOJISTAR(pandas.Series(opens) , pandas.Series(highs) , pandas.Series(lows) , pandas.Series(closes))
        if (evening_doji_start[-1] != 0):
            print("{} - evening doji start pattern detected at -> {}".format(data['s'] , date))

        # -------------------------------------------------------------------------------------

        evening_star = talib.CDLEVENINGSTAR(pandas.Series(opens) , pandas.Series(highs) , pandas.Series(lows) , pandas.Series(closes))
        if (evening_star[-1] != 0):
            print("{} - evening star pattern detected at -> {}".format(data['s'] , date))

        # -------------------------------------------------------------------------------------

        gap_side = talib.CDLGAPSIDESIDEWHITE(pandas.Series(opens) , pandas.Series(highs) , pandas.Series(lows) , pandas.Series(closes))
        if (gap_side[-1] != 0):
            print("{} - up/down-gap side-by-side white lines pattern detected at -> {}".format(data['s'] , date))

        # -------------------------------------------------------------------------------------

        grave_stone_doji = talib.CDLGRAVESTONEDOJI(pandas.Series(opens) , pandas.Series(highs) , pandas.Series(lows) , pandas.Series(closes))
        if (grave_stone_doji[-1] != 0):
            print("{} - grave stone doji pattern detected at -> {}".format(data['s'] , date))

        # -------------------------------------------------------------------------------------

        hammer = talib.CDLHAMMER(pandas.Series(opens) , pandas.Series(highs) , pandas.Series(lows) , pandas.Series(closes))
        if (hammer[-1] != 0):
            print("{} - hammer pattern detected at -> {}".format(data['s'] , date))

        # -------------------------------------------------------------------------------------

        hanging_man = talib.CDLHANGINGMAN(pandas.Series(opens) , pandas.Series(highs) , pandas.Series(lows) , pandas.Series(closes))
        if (hanging_man[-1] != 0):
            print("{} - hanging man pattern detected at -> {}".format(data['s'] , date))

        # -------------------------------------------------------------------------------------

        harami = talib.CDLHARAMI(pandas.Series(opens) , pandas.Series(highs) , pandas.Series(lows) , pandas.Series(closes))
        if (harami[-1] != 0):
            print("{} - harami pattern detected at -> {}".format(data['s'] , date))

        # -------------------------------------------------------------------------------------

        harami_cross = talib.CDLHARAMICROSS(pandas.Series(opens) , pandas.Series(highs) , pandas.Series(lows) , pandas.Series(closes))
        if (two_crows[-1] != 0):
            print("{} - harami cross pattern detected at -> {}".format(data['s'] , date))

        # -------------------------------------------------------------------------------------

        high_wave_candle = talib.CDLHIGHWAVE(pandas.Series(opens) , pandas.Series(highs) , pandas.Series(lows) , pandas.Series(closes))
        if (high_wave_candle[-1] != 0):
            print("{} - high wave candle pattern detected at -> {}".format(data['s'] , date))

        # -------------------------------------------------------------------------------------

        hikkake = talib.CDLHIKKAKE(pandas.Series(opens) , pandas.Series(highs) , pandas.Series(lows) , pandas.Series(closes))
        if (hikkake[-1] != 0):
            print("{} - hikkake pattern detected at -> {}".format(data['s'] , date))

        # -------------------------------------------------------------------------------------

        modified_hikkake = talib.CDLHIKKAKEMOD(pandas.Series(opens) , pandas.Series(highs) , pandas.Series(lows) , pandas.Series(closes))
        if (modified_hikkake[-1] != 0):
            print("{} - modified hikkake pattern detected at -> {}".format(data['s'] , date))

        # -------------------------------------------------------------------------------------

        homing_pigeon = talib.CDLHOMINGPIGEON(pandas.Series(opens) , pandas.Series(highs) , pandas.Series(lows) , pandas.Series(closes))
        if (homing_pigeon[-1] != 0):
            print("{} - homing pigeon pattern detected at -> {}".format(data['s'] , date))

        # -------------------------------------------------------------------------------------

        identical_three_crows = talib.CDLIDENTICAL3CROWS(pandas.Series(opens) , pandas.Series(highs) , pandas.Series(lows) , pandas.Series(closes))
        if (identical_three_crows[-1] != 0):
            print("{} - identical three crows pattern detected at -> {}".format(data['s'] , date))

        # -------------------------------------------------------------------------------------

        in_neck = talib.CDLINNECK(pandas.Series(opens) , pandas.Series(highs) , pandas.Series(lows) , pandas.Series(closes))
        if (in_neck[-1] != 0):
            print("{} - in neck pattern detected at -> {}".format(data['s'] , date))

        # -------------------------------------------------------------------------------------

        inverted_hammer = talib.CDLINVERTEDHAMMER(pandas.Series(opens) , pandas.Series(highs) , pandas.Series(lows) , pandas.Series(closes))
        if (inverted_hammer[-1] != 0):
            print("{} - inverted hammer pattern detected at -> {}".format(data['s'] , date))

        # -------------------------------------------------------------------------------------

        kicking = talib.CDLKICKING(pandas.Series(opens) , pandas.Series(highs) , pandas.Series(lows) , pandas.Series(closes))
        if (kicking[-1] != 0):
            print("{} - kicking pattern detected at -> {}".format(data['s'] , date))

        # -------------------------------------------------------------------------------------

        kicking_by_len = talib.CDLKICKINGBYLENGTH(pandas.Series(opens) , pandas.Series(highs) , pandas.Series(lows) , pandas.Series(closes))
        if (kicking_by_len[-1] != 0):
            print("{} - kicking - bull/bear determined by the longer marubozu pattern detected at -> {}".format(data['s'] , date))

        # -------------------------------------------------------------------------------------

        ladder_bottom = talib.CDLLADDERBOTTOM(pandas.Series(opens) , pandas.Series(highs) , pandas.Series(lows) , pandas.Series(closes))
        if (ladder_bottom[-1] != 0):
            print("{} - ladder bottom pattern detected at -> {}".format(data['s'] , date))

        # -------------------------------------------------------------------------------------

        long_legged_doji = talib.CDLLONGLEGGEDDOJI(pandas.Series(opens) , pandas.Series(highs) , pandas.Series(lows) , pandas.Series(closes))
        if (long_legged_doji[-1] != 0):
            print("{} - long legged doji pattern detected at -> {}".format(data['s'] , date))

        # -------------------------------------------------------------------------------------

        long_line_candle = talib.CDLLONGLINE(pandas.Series(opens) , pandas.Series(highs) , pandas.Series(lows) , pandas.Series(closes))
        if (long_line_candle[-1] != 0):
            print("{} - long line candle pattern detected at -> {}".format(data['s'] , date))

        # -------------------------------------------------------------------------------------

        marubozu = talib.CDLMARUBOZU(pandas.Series(opens) , pandas.Series(highs) , pandas.Series(lows) , pandas.Series(closes))
        if (marubozu[-1] != 0):
            print("{} - marubozu pattern detected at -> {}".format(data['s'] , date))

        # -------------------------------------------------------------------------------------

        matching_low = talib.CDLMATCHINGLOW(pandas.Series(opens) , pandas.Series(highs) , pandas.Series(lows) , pandas.Series(closes))
        if (matching_low[-1] != 0):
            print("{} - matching low pattern detected at -> {}".format(data['s'] , date))

        # -------------------------------------------------------------------------------------

        mat_hold = talib.CDLMATHOLD(pandas.Series(opens) , pandas.Series(highs) , pandas.Series(lows) , pandas.Series(closes))
        if (mat_hold[-1] != 0):
            print("{} - mat hold pattern detected at -> {}".format(data['s'] , date))

        # -------------------------------------------------------------------------------------

        morning_doji_star = talib.CDLMORNINGDOJISTAR(pandas.Series(opens) , pandas.Series(highs) , pandas.Series(lows) , pandas.Series(closes))
        if (morning_doji_star[-1] != 0):
            print("{} - morning doji star pattern detected at -> {}".format(data['s'] , date))

        # -------------------------------------------------------------------------------------

        morning_star = talib.CDLMORNINGSTAR(pandas.Series(opens) , pandas.Series(highs) , pandas.Series(lows) , pandas.Series(closes))
        if (morning_star[-1] != 0):
            print("{} - morning star pattern detected at -> {}".format(data['s'] , date))

        # -------------------------------------------------------------------------------------

        on_neck = talib.CDLONNECK(pandas.Series(opens) , pandas.Series(highs) , pandas.Series(lows) , pandas.Series(closes))
        if (on_neck[-1] != 0):
            print("{} - on neck pattern detected at -> {}".format(data['s'] , date))

        # -------------------------------------------------------------------------------------

        piercing = talib.CDLPIERCING(pandas.Series(opens) , pandas.Series(highs) , pandas.Series(lows) , pandas.Series(closes))
        if (piercing[-1] != 0):
            print("{} - piercing pattern detected at -> {}".format(data['s'] , date))

        # -------------------------------------------------------------------------------------

        rickshaw_man = talib.CDLRICKSHAWMAN(pandas.Series(opens) , pandas.Series(highs) , pandas.Series(lows) , pandas.Series(closes))
        if (rickshaw_man[-1] != 0):
            print("{} - rick shaw man pattern detected at -> {}".format(data['s'] , date))

        # -------------------------------------------------------------------------------------

        rise_fall_three_methods = talib.CDLRISEFALL3METHODS(pandas.Series(opens) , pandas.Series(highs) , pandas.Series(lows) , pandas.Series(closes))
        if (rise_fall_three_methods[-1] != 0):
            print("{} - rise fall three methods pattern detected at -> {}".format(data['s'] , date))

        # -------------------------------------------------------------------------------------

        seprating_line = talib.CDLSEPARATINGLINES(pandas.Series(opens) , pandas.Series(highs) , pandas.Series(lows) , pandas.Series(closes))
        if (seprating_line[-1] != 0):
            print("{} - seprating line pattern detected at -> {}".format(data['s'] , date))

        # -------------------------------------------------------------------------------------

        shooting_star = talib.CDLSHOOTINGSTAR(pandas.Series(opens) , pandas.Series(highs) , pandas.Series(lows) , pandas.Series(closes))
        if (shooting_star[-1] != 0):
            print("{} - shooting star pattern detected at -> {}".format(data['s'] , date))

        # -------------------------------------------------------------------------------------

        short_line = talib.CDLSHORTLINE(pandas.Series(opens) , pandas.Series(highs) , pandas.Series(lows) , pandas.Series(closes))
        if (short_line[-1] != 0):
            print("{} - short line pattern detected at -> {}".format(data['s'] , date))

        # -------------------------------------------------------------------------------------

        spinning_top = talib.CDLSPINNINGTOP(pandas.Series(opens) , pandas.Series(highs) , pandas.Series(lows) , pandas.Series(closes))
        if (spinning_top[-1] != 0):
            print("{} - spinning top pattern detected at -> {}".format(data['s'] , date))

        # -------------------------------------------------------------------------------------

        sattled = talib.CDLSTALLEDPATTERN(pandas.Series(opens) , pandas.Series(highs) , pandas.Series(lows) , pandas.Series(closes))
        if (sattled[-1] != 0):
            print("{} - sattled pattern detected at -> {}".format(data['s'] , date))

        # -------------------------------------------------------------------------------------

        stick_sandwich = talib.CDLSTICKSANDWICH(pandas.Series(opens) , pandas.Series(highs) , pandas.Series(lows) , pandas.Series(closes))
        if (stick_sandwich[-1] != 0):
            print("{} - stick sandwich pattern detected at -> {}".format(data['s'] , date))

        # -------------------------------------------------------------------------------------

        takuri = talib.CDLTAKURI(pandas.Series(opens) , pandas.Series(highs) , pandas.Series(lows) , pandas.Series(closes))
        if (takuri[-1] != 0):
            print("{} - takuri pattern detected at -> {}".format(data['s'] , date))

        # -------------------------------------------------------------------------------------

        tasuki_gap = talib.CDLTASUKIGAP(pandas.Series(opens) , pandas.Series(highs) , pandas.Series(lows) , pandas.Series(closes))
        if (tasuki_gap[-1] != 0):
            print("{} - tasuki gap pattern detected at -> {}".format(data['s'] , date))

        # -------------------------------------------------------------------------------------

        thrusting = talib.CDLTHRUSTING(pandas.Series(opens) , pandas.Series(highs) , pandas.Series(lows) , pandas.Series(closes))
        if (thrusting[-1] != 0):
            print("{} - thrusting pattern detected at -> {}".format(data['s'] , date))

        # -------------------------------------------------------------------------------------

        tristar = talib.CDLTRISTAR(pandas.Series(opens) , pandas.Series(highs) , pandas.Series(lows) , pandas.Series(closes))
        if (tristar[-1] != 0):
            print("{} - tristar pattern detected at -> {}".format(data['s'] , date))

        # -------------------------------------------------------------------------------------

        unique_three_river = talib.CDLUNIQUE3RIVER(pandas.Series(opens) , pandas.Series(highs) , pandas.Series(lows) , pandas.Series(closes))
        if (unique_three_river[-1] != 0):
            print("{} - unique three river pattern detected at -> {}".format(data['s'] , date))

        # -------------------------------------------------------------------------------------

        upside_gap_two_crows = talib.CDLUPSIDEGAP2CROWS(pandas.Series(opens) , pandas.Series(highs) , pandas.Series(lows) , pandas.Series(closes))
        if (upside_gap_two_crows[-1] != 0):
            print("{} - upside gap two crows pattern detected at -> {}".format(data['s'] , date))

        # -------------------------------------------------------------------------------------

        upside_down_gap_three_methods = talib.CDLXSIDEGAP3METHODS(pandas.Series(opens) , pandas.Series(highs) , pandas.Series(lows) , pandas.Series(closes))
        if (upside_down_gap_three_methods[-1] != 0):
            print("{} - upside down gap three methods pattern detected at -> {}".format(data['s'] , date))

# -------------------------------------------------------------------------------------

detect()