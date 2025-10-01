from playwright.sync_api import sync_playwright, expect
import time

def run_verification(playwright):
    browser = playwright.chromium.launch(headless=True)
    context = browser.new_context()
    page = context.new_page()

    try:
        # 1. Go to dashboard
        page.goto("http://127.0.0.1:5000/dashboard")
        expect(page.get_by_role("heading", name="Dashboard de Migração de Clientes")).to_be_visible()
        page.screenshot(path="jules-scratch/verification/01_dashboard_initial.png")

        # 2. Go to add client page
        page.get_by_role("link", name="Adicionar Novo Cliente").click()
        expect(page.get_by_role("heading", name="Cadastrar Novo Cliente")).to_be_visible()

        # 3. Fill form and submit
        page.get_by_label("Nome do Cliente *").fill("Cliente Teste Playwright")
        page.get_by_label("Código Único *").fill("PLW01")
        page.get_by_label("CNPJ").fill("12345678000199")
        page.get_by_role("button", name="Salvar Cliente").click()

        # 4. Verify client was created on dashboard
        expect(page.get_by_role("heading", name="Dashboard de Migração de Clientes")).to_be_visible()
        expect(page.get_by_text("Cliente Teste Playwright (PLW01)")).to_be_visible()
        page.screenshot(path="jules-scratch/verification/02_dashboard_with_client.png")

        # 5. Go to client details page
        page.get_by_role("link", name="Gerenciar Migração").click()
        expect(page.get_by_role("heading", name="Migração de Dados - Cliente Teste Playwright")).to_be_visible()

        # 6. Verify details page content
        expect(page.get_by_text("CNPJ: 12345678000199")).to_be_visible()
        expect(page.get_by_role("heading", name="Histórico de Processamento")).to_be_visible()
        expect(page.get_by_text("Nenhum arquivo foi processado para este cliente ainda.")).to_be_visible()

        page.screenshot(path="jules-scratch/verification/03_client_details.png")
        print("Verification script completed successfully.")

    except Exception as e:
        print(f"An error occurred: {e}")
        page.screenshot(path="jules-scratch/verification/error.png")

    finally:
        browser.close()

with sync_playwright() as playwright:
    run_verification(playwright)