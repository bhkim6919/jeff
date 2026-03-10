
class StopManager:
    def __init__(self, portfolio, price_provider):
        self.portfolio = portfolio
        self.price_provider = price_provider

    def check_exits(self):
        signals = []

        for code, pos in self.portfolio.positions.items():
            current_price = self.price_provider.get_last_price(code)
            ma20 = self.price_provider.get_ma20(code)

            if pos.tp and current_price >= pos.tp:
                signals.append({"code": code, "reason": "TP", "price": current_price})
                continue

            if pos.sl and current_price <= pos.sl:
                signals.append({"code": code, "reason": "SL", "price": current_price})
                continue

            if ma20 and current_price < ma20:
                signals.append({"code": code, "reason": "MA_BREAK", "price": current_price})

        return signals
