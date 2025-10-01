from playwright.sync_api import sync_playwright, expect

def run_verification():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        try:
            # Navigate to the application's home page
            page.goto("http://127.0.0.1:5000/")

            # Wait for the main dashboard container to be visible
            dashboard_container = page.locator(".dashboard-container")
            expect(dashboard_container).to_be_visible(timeout=10000)

            # Check for the header text
            header = page.get_by_role("heading", name="Dashboard de Organizações")
            expect(header).to_be_visible()

            # Take a screenshot of the dashboard
            page.screenshot(path="jules-scratch/verification/dashboard_verification.png")

            print("Screenshot captured successfully.")

        except Exception as e:
            print(f"An error occurred during verification: {e}")

        finally:
            browser.close()

if __name__ == "__main__":
    run_verification()