from datetime import datetime

class ResultDto:
    hkjc_id = None
    ht_time = None
    ht_odd = None
    ht_prematch_odd = None
    ht_prematch_goalline = None
    ht_rise = None
    ht_success = None
    ft_time = None
    ft_odd = None
    ft_success = None
    ft_prematch_odd = None
    ft_prematch_goalline = None
    ft_rise = None
    ft_last_min = None
    ht_last_min = None
    match_date :datetime = None
    ht_prob = None
    ft_prob = None
    ht_pred = None
    id = None
    
    def __init__(self, hkjc_id, ht_time, ht_odd):
        self.hkjc_id = hkjc_id
        self.ht_time = ht_time
        self.ht_odd = ht_odd