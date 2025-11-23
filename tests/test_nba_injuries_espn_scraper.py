"""Tests for NBA injury scraper."""
import unittest
from unittest.mock import MagicMock, patch

from src.data.sources.nba_injuries_espn_scraper import scrape_injuries


class TestNBAInjuriesScraper(unittest.TestCase):
    @patch("src.data.sources.nba_injuries_espn_scraper.requests.get")
    def test_scrape_injuries_success(self, mock_get):
        mock_response = MagicMock()
        mock_response.content = """
        <html>
            <body>
                <div class="ResponsiveTable">
                    <div class="Table__Title">Atlanta Hawks</div>
                    <table class="Table">
                        <tbody>
                            <tr>
                                <td><a href="https://www.espn.com/nba/player/_/id/12345/trae-young">Trae Young</a></td>
                                <td>PG</td>
                                <td>Nov 19</td>
                                <td>Out</td>
                                <td>Ankle</td>
                            </tr>
                        </tbody>
                    </table>
                </div>
            </body>
        </html>
        """
        mock_get.return_value = mock_response

        injuries = scrape_injuries()
        self.assertEqual(len(injuries), 1)
        self.assertEqual(injuries[0]["team"], "Atlanta Hawks")
        self.assertEqual(injuries[0]["player_name"], "Trae Young")
        self.assertEqual(injuries[0]["player_id"], "12345")
        self.assertEqual(injuries[0]["status"], "Out")
        self.assertEqual(injuries[0]["date"], "Nov 19")

    @patch("src.data.sources.nba_injuries_espn_scraper.requests.get")
    def test_scrape_injuries_failure(self, mock_get):
        import requests
        mock_get.side_effect = requests.RequestException("Network error")
        injuries = scrape_injuries()
        self.assertEqual(len(injuries), 0)

if __name__ == "__main__":
    unittest.main()
