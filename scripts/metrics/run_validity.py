# TODO: Update type-hint or use separate fn
def is_runnable(source_code: str, verbose: bool = False) -> bool:
    try:
        compiled_code = compile(source_code, "<string>", "exec")
        exec(compiled_code)
        if verbose:
            return True, ""
        return True
    except Exception as e:
        if verbose:
            return False, f"Failed to run code\n\tError: {e}"
        return False
