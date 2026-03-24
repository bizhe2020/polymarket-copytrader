import unittest

from polymarket_copytrader.resolve import _score_profile, normalize_profile_input


class ResolveTests(unittest.TestCase):
    def test_normalize_profile_input(self) -> None:
        self.assertEqual(
            normalize_profile_input("https://polymarket.com/zh/profile/%40guh123"),
            "guh123",
        )
        self.assertEqual(
            normalize_profile_input("https://polymarket.com/zh/@blue-walnut?tab=activity"),
            "blue-walnut",
        )
        self.assertEqual(normalize_profile_input("@guh123"), "guh123")

    def test_score_exact_profile_match(self) -> None:
        profile = {"name": "guh123", "pseudonym": "GUH123"}
        self.assertEqual(_score_profile(profile, "guh123"), 100)


if __name__ == "__main__":
    unittest.main()
