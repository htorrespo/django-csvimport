from django import dispatch

imported_csv = dispatch.Signal(providing_args=['instance', 'created', 'row'])
importing_csv = dispatch.Signal(providing_args=['instance', 'row'])
