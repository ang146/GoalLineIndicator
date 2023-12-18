class ResultDto:
    id = None
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
    
    def __init__(self, id, ht_time, ht_odd):
        self.id = id
        self.ht_time = ht_time
        self.ht_odd = ht_odd