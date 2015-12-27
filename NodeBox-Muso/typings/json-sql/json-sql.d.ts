interface result {
	query: string;
	values: any;
	prefixValues(): any;
	getValuesArray(): any[];
	getValuesObject: any;
}
interface mongosql {
	sql(query: any, values: any[]): result;
	toString(): string;
	toQuery(): result;
}