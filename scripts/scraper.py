if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        import traceback
        tb = traceback.format_exc()
        # Always leave an artifact with the traceback so we can see what happened
        with open("last_report.html", "w", encoding="utf-8") as f:
            f.write(f"""<html><body>
            <h2>Delran BOE – Monitor: Unhandled Error</h2>
            <pre style="white-space: pre-wrap; font-family: monospace;">{tb}</pre>
            </body></html>""")
        print("Unhandled error; traceback written to last_report.html")
        raise
