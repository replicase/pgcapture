package decode

type SchemaLoader interface {
	GetFieldOID(namespace, table, field string) (uint32, error)
}

type ZeroSchemaLoader struct{}

func (z *ZeroSchemaLoader) GetFieldOID(namespace, table, field string) (uint32, error) {
	return 0, nil
}
